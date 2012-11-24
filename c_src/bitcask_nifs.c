// -------------------------------------------------------------------
//
// bitcask: Eric Brewer-inspired key/value store
//
// Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdint.h>

#include "erl_nif.h"
#include "erl_driver.h"
#include "erl_nif_util.h"

#include "async_nif.h"

#include "khash.h"
#include "murmurhash.h"

#include <stdio.h>
#ifdef BITCASK_DEBUG
#include <stdio.h>
#include <stdarg.h>
void DEBUG(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fflush(stderr);
}
#else
#  define DEBUG(X, ...) {}
#endif

#ifdef PULSE
#include "pulse_c_send.h"
#endif

static ErlNifResourceType* bitcask_keydir_RESOURCE;

static ErlNifResourceType* bitcask_lock_RESOURCE;

static ErlNifResourceType* bitcask_file_RESOURCE;

typedef struct
{
    int fd;
} bitcask_file_handle;

typedef struct
{
    uint32_t file_id;
    uint32_t total_sz;
    uint64_t offset;
    uint32_t tstamp;
    uint32_t newest_put;
    uint16_t key_sz;
    char     key[0];
} bitcask_keydir_entry;


static khint_t keydir_entry_hash(bitcask_keydir_entry* entry);
static khint_t keydir_entry_equal(bitcask_keydir_entry* lhs,
                                  bitcask_keydir_entry* rhs);
KHASH_INIT(entries, bitcask_keydir_entry*, char, 0, keydir_entry_hash, keydir_entry_equal);

typedef struct
{
    uint32_t file_id;
    uint64_t live_keys;   // number of 'live' keys in entries and pending
    uint64_t live_bytes;  // number of 'live' bytes
    uint64_t total_keys;  // total number of keys written to file
    uint64_t total_bytes; // total number of bytes written to file
    uint32_t oldest_tstamp; // oldest observed tstamp in a file
    uint32_t newest_tstamp; // newest observed tstamp in a file
} bitcask_fstats_entry;

KHASH_MAP_INIT_INT(fstats, bitcask_fstats_entry*);

typedef khash_t(entries) entries_hash_t;
typedef khash_t(fstats) fstats_hash_t;

typedef struct
{
    entries_hash_t* entries;
    entries_hash_t* pending;  // pending keydir entries during keydir folding
    fstats_hash_t*  fstats;
    size_t        key_count;
    size_t        key_bytes;
    uint32_t      biggest_file_id;
    unsigned int  refcount;
    unsigned int  keyfolders;
    uint64_t      iter_generation;
    uint64_t      pending_updated;
    uint64_t      pending_start; // os:timestamp() as 64-bit integer
    ErlNifPid*    pending_awaken; // processes to wake once pending merged into entries
    unsigned int  pending_awaken_count;
    unsigned int  pending_awaken_size;
    ErlNifMutex*  mutex;
    char          is_ready;
    char          name[0];
} bitcask_keydir;

typedef struct
{
    bitcask_keydir* keydir;
    int             iterating;
    khiter_t        iterator;
} bitcask_keydir_handle;

typedef struct
{
    int   fd;
    int   is_write_lock;
    char  filename[0];
} bitcask_lock_handle;

KHASH_INIT(global_keydirs, char*, bitcask_keydir*, 1, kh_str_hash_func, kh_str_hash_equal);

typedef struct
{
    khash_t(global_keydirs)* global_keydirs;
    ErlNifMutex*             global_keydirs_lock;
} bitcask_priv_data;

#define kh_put2(name, h, k, v) {                        \
        int itr_status;                                 \
        khiter_t itr = kh_put(name, h, k, &itr_status); \
        kh_val(h, itr) = v; }                           \

#define kh_put_set(name, h, k) {                        \
        int itr_status;                                 \
        kh_put(name, h, k, &itr_status); }


// Handle lock helper functions
#define LOCK(keydir)      { if (keydir->mutex) enif_mutex_lock(keydir->mutex); }
#define UNLOCK(keydir)    { if (keydir->mutex) enif_mutex_unlock(keydir->mutex); }

// Pending tombstones
#define is_pending_tombstone(e) ((e)->tstamp == 0 &&   \
                                 (e)->offset == 0)
#define set_pending_tombstone(e) {(e)->tstamp = 0; \
                                  (e)->offset = 0; }

// Atoms (using enif_make_atom everywhere for thread-safety)
#define ATOM_ALLOCATION_ERROR enif_make_atom(env, "allocation_error")
#define ATOM_ALREADY_EXISTS enif_make_atom(env, "already_exists")
#define ATOM_BITCASK_ENTRY enif_make_atom(env, "bitcask_entry")
#define ATOM_ERROR enif_make_atom(env, "error")
#define ATOM_FALSE enif_make_atom(env, "false")
#define ATOM_FSTAT_ERROR enif_make_atom(env, "fstat_error")
#define ATOM_FTRUNCATE_ERROR enif_make_atom(env, "ftruncate_error")
#define ATOM_GETFL_ERROR enif_make_atom(env, "getfl_error")
/* Iteration lock thread creation error */
#define ATOM_ILT_CREATE_ERROR enif_make_atom(env, "ilt_create_error")
#define ATOM_ITERATION_IN_PROCESS enif_make_atom(env, "iteration_in_process")
#define ATOM_ITERATION_NOT_PERMITTED enif_make_atom(env, "iteration_not_permitted")
#define ATOM_ITERATION_NOT_STARTED enif_make_atom(env, "iteration_not_started")
#define ATOM_LOCK_NOT_WRITABLE enif_make_atom(env, "lock_not_writable")
#define ATOM_NOT_FOUND enif_make_atom(env, "not_found")
#define ATOM_NOT_READY enif_make_atom(env, "not_ready")
#define ATOM_OK enif_make_atom(env, "ok")
#define ATOM_OUT_OF_DATE enif_make_atom(env, "out_of_date")
#define ATOM_PREAD_ERROR enif_make_atom(env, "pread_error")
#define ATOM_PWRITE_ERROR enif_make_atom(env, "pwrite_error")
#define ATOM_READY enif_make_atom(env, "ready")
#define ATOM_SETFL_ERROR enif_make_atom(env, "setfl_error")
#define ATOM_TRUE enif_make_atom(env, "true")
#define ATOM_EOF enif_make_atom(env, "eof")
#define ATOM_CREATE enif_make_atom(env, "create")
#define ATOM_READONLY enif_make_atom(env, "readonly")
#define ATOM_O_SYNC enif_make_atom(env, "o_sync")

ERL_NIF_TERM errno_atom(ErlNifEnv* env, int error);
ERL_NIF_TERM errno_error_tuple(ErlNifEnv* env, ERL_NIF_TERM key, int error);

static void merge_pending_entries(ErlNifEnv* env, bitcask_keydir* keydir);
static void lock_release(bitcask_lock_handle* handle);

static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg);
static void bitcask_nifs_keydir_priv_resource_cleanup(ErlNifEnv* env, bitcask_keydir_handle *handle, bitcask_priv_data* priv);
static void bitcask_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg);


ASYNC_NIF_DECL(
    bitcask_nifs_keydir_new0,
    { // struct
    },
    { // pre
    },
    { // work

      bitcask_keydir_handle* handle;

      // First, setup a resource for our handle
      handle = enif_alloc_resource(bitcask_keydir_RESOURCE,
                                   sizeof(bitcask_keydir_handle));
      memset(handle, 0, sizeof(bitcask_keydir_handle));

      // Now allocate the actual keydir instance. Because it's unnamed/shared, we'll
      // leave the name and lock portions null'd out
      bitcask_keydir* keydir = enif_alloc(sizeof(bitcask_keydir));
      memset(keydir, 0, sizeof(bitcask_keydir));
      keydir->entries  = kh_init(entries);
      keydir->fstats   = kh_init(fstats);

      // Assign the keydir to our handle and hand it back
      handle->keydir = keydir;
      ERL_NIF_TERM result = enif_make_resource(env, handle);
      enif_release_resource(handle);
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
    },
    { // post

    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_new1,
    { // struct

      char name[4096];
      size_t name_sz;
    },
    { // pre

      if (!(enif_get_string(env, argv[0], args->name, sizeof(args->name), ERL_NIF_LATIN1) > 0)) {
        ASYNC_NIF_RETURN_BADARG();
      }
      args->name_sz = strlen(args->name);
    },
    { // work

      // Get our private stash and check the global hash table for this entry
      bitcask_priv_data* priv = (bitcask_priv_data*)priv_data;
      enif_mutex_lock(priv->global_keydirs_lock);

      bitcask_keydir* keydir;
      khiter_t itr = kh_get(global_keydirs, priv->global_keydirs, args->name);
      if (itr != kh_end(priv->global_keydirs)) {
        keydir = kh_val(priv->global_keydirs, itr);
        // Existing keydir is available. Check the is_ready flag to determine if
        // the original creator is ready for other processes to use it.
        if (!keydir->is_ready) {
          // Notify the caller that while the requested keydir exists, it's not
          // ready for public usage.
          enif_mutex_unlock(priv->global_keydirs_lock);
          ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_NOT_READY));
        } else {
          keydir->refcount++;
        }
      } else {
        // No such keydir, create a new one and add to the globals list. Make sure
        // to allocate enough room for the name.
        keydir = enif_alloc(sizeof(bitcask_keydir) + args->name_sz + 1);
        memset(keydir, 0, sizeof(bitcask_keydir) + args->name_sz + 1);
        strncpy(keydir->name, args->name, args->name_sz + 1);

        // Initialize hash tables
        keydir->entries  = kh_init(entries);
        keydir->fstats   = kh_init(fstats);

        // Be sure to initialize the mutex and set our refcount
        keydir->mutex = enif_mutex_create(args->name);
        keydir->refcount = 1;

        // Finally, register this new keydir in the globals
        kh_put2(global_keydirs, priv->global_keydirs, keydir->name, keydir);
      }

      enif_mutex_unlock(priv->global_keydirs_lock);

      // Setup a resource for the handle
      bitcask_keydir_handle* handle;
      handle = enif_alloc_resource(bitcask_keydir_RESOURCE,
                                   sizeof(bitcask_keydir_handle));
      memset(handle, 0, sizeof(bitcask_keydir_handle));
      handle->keydir = keydir;
      ERL_NIF_TERM result = enif_make_resource(env, handle);
      enif_release_resource(handle);

      // Return to the caller a tuple with the reference and an atom
      // indicating if the keydir is ready or not.
      ERL_NIF_TERM is_ready_atom = keydir->is_ready ? ATOM_READY : ATOM_NOT_READY;
      ASYNC_NIF_REPLY(enif_make_tuple2(env, is_ready_atom, result));
    },
    { // post
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_mark_ready,
    { // struct

      bitcask_keydir_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle)) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      bitcask_keydir* keydir = args->handle->keydir;
      LOCK(keydir);
      keydir->is_ready = 1;
      UNLOCK(keydir);
      ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

static void update_fstats(ErlNifEnv* env, bitcask_keydir* keydir,
                          uint32_t file_id, uint32_t tstamp,
                          int32_t live_increment, int32_t total_increment,
                          int32_t live_bytes_increment, int32_t total_bytes_increment)
{
    bitcask_fstats_entry* entry = 0;
    khiter_t itr = kh_get(fstats, keydir->fstats, file_id);
    if (itr == kh_end(keydir->fstats))
    {
        // Need to initialize new entry and add to the table
        entry = enif_alloc(sizeof(bitcask_fstats_entry));
        memset(entry, 0, sizeof(bitcask_fstats_entry));
        entry->file_id = file_id;

        kh_put2(fstats, keydir->fstats, file_id, entry);
    }
    else
    {
        entry = kh_val(keydir->fstats, itr);
    }

    entry->live_keys   += live_increment;
    entry->total_keys  += total_increment;
    entry->live_bytes  += live_bytes_increment;
    entry->total_bytes += total_bytes_increment;

    if ((tstamp != 0 && tstamp < entry->oldest_tstamp) ||
        entry->oldest_tstamp == 0)
    {
        entry->oldest_tstamp = tstamp;
    }
    if ((tstamp != 0 && tstamp > entry->newest_tstamp) ||
        entry->newest_tstamp == 0)
    {
        entry->newest_tstamp = tstamp;
    }
}

static khint_t keydir_entry_hash(bitcask_keydir_entry* entry)
{
    return MURMUR_HASH(entry->key, entry->key_sz, 42);
}

static khint_t keydir_entry_equal(bitcask_keydir_entry* lhs,
                                  bitcask_keydir_entry* rhs)
{
    if (lhs->key_sz != rhs->key_sz)
    {
        return 0;
    }
    else
    {
        return (memcmp(lhs->key, rhs->key, lhs->key_sz) == 0);
    }
}

static khiter_t get_entries_hash(ErlNifEnv* env, entries_hash_t *hash, ErlNifBinary* key,
                                 khiter_t* itr_ptr, bitcask_keydir_entry** entry_ptr)
{
    khiter_t itr;
    if (key->size < (4096 - sizeof(bitcask_keydir_entry)))
    {
        char buf[4096];
        bitcask_keydir_entry* e = (bitcask_keydir_entry*)buf;
        e->key_sz = key->size;
        memcpy(e->key, key->data, key->size);
        itr = kh_get(entries, hash, e);
    }
    else
    {
        bitcask_keydir_entry* e = enif_alloc(sizeof(bitcask_keydir_entry) +
                                             key->size);
        memset(e, 0, sizeof(bitcask_keydir_entry));
        e->key_sz = key->size;
        memcpy(e->key, key->data, key->size);
        itr = kh_get(entries, hash, e);
        enif_free(e);
    }

    if (itr != kh_end(hash))
    {
        if (itr_ptr != NULL)
        {
            *itr_ptr = itr;
        }
        if (entry_ptr != NULL)
        {
            *entry_ptr = kh_key(hash, itr);
        }
        return 1;
    }
    else
    {
        return 0;
    }
}

// Find an entry in the pending or entries keydir and update the hash/itr/entry pointers
// if non-NULL.  If iterating is true, restrict search to the frozen keydir.
static int find_keydir_entry(ErlNifEnv* env, bitcask_keydir* keydir, ErlNifBinary* key,
                             entries_hash_t** hash_ptr, khiter_t* itr_ptr,
                             bitcask_keydir_entry** entry_ptr, int iterating)
{
    // Search pending if present
    if (keydir->pending != NULL && !iterating)
    {
        if (get_entries_hash(env, keydir->pending, key, itr_ptr, entry_ptr))
        {
            if (hash_ptr != NULL)
            {
                *hash_ptr = keydir->pending;
            }
            return 1;
        }
    }
    // If not in pending, check normal entries
    if (get_entries_hash(env, keydir->entries, key, itr_ptr, entry_ptr))
    {
        if (hash_ptr != NULL)
        {
            *hash_ptr = keydir->entries;
        }
        return 1;
    }

    // Not in entries or pending
    return 0;
}

// Allocate, populate and add entry to the keydir hash based on the key and entry structure
static bitcask_keydir_entry* add_entry(ErlNifEnv* env, bitcask_keydir* keydir,
                      entries_hash_t* hash,
                      ErlNifBinary* key, bitcask_keydir_entry* entry)
{
    bitcask_keydir_entry* new_entry = enif_alloc(sizeof(bitcask_keydir_entry) +
                                                 key->size);
    memset(new_entry, 0, sizeof(bitcask_keydir_entry));
    new_entry->file_id = entry->file_id;
    new_entry->total_sz = entry->total_sz;
    new_entry->offset = entry->offset;
    new_entry->tstamp = entry->tstamp;
    new_entry->key_sz = key->size;
    memcpy(new_entry->key, key->data, key->size);
    kh_put_set(entries, hash, new_entry);

    return new_entry;
}

// Move an entry from pending into entries
static void move_pending_entry(ErlNifEnv* env, bitcask_keydir* keydir,
                               khiter_t pend_itr, bitcask_keydir_entry* entry)
{
    kh_put_set(entries, keydir->entries, entry);
    // no need to delete from pending entry, it will be freed as a whole
}


// Update the current entry with newer information
static void update_entry(ErlNifEnv* env, bitcask_keydir* keydir,
                         bitcask_keydir_entry* cur_entry,
                         bitcask_keydir_entry* upd_entry)
{
    cur_entry->file_id = upd_entry->file_id;
    cur_entry->total_sz = upd_entry->total_sz;
    cur_entry->offset = upd_entry->offset;
    cur_entry->tstamp = upd_entry->tstamp;
}

static void remove_entry(ErlNifEnv* env, bitcask_keydir* keydir, khiter_t itr,
                         bitcask_keydir_entry* entry)
{
    kh_del(entries, keydir->entries, itr);
}

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_put,
    { // struct

      bitcask_keydir_handle* handle;
      bitcask_keydir_entry entry;
      ErlNifBinary key;
      uint32_t old_file_id;
      uint64_t old_offset;
    },
    { // pre

      if (!(enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle) &&
            enif_inspect_binary(env, argv[1], &args->key) &&
            enif_get_uint(env, argv[2], (unsigned int*)&(args->entry.file_id)) &&
            enif_get_uint(env, argv[3], &(args->entry.total_sz)) &&
            enif_get_uint64_bin(env, argv[4], &(args->entry.offset)) &&
            enif_get_uint(env, argv[5], &(args->entry.tstamp)) &&
            enif_get_uint(env, argv[6], &(args->entry.newest_put)) &&
            enif_get_uint(env, argv[7], (unsigned int*)&(args->old_file_id)) &&
            enif_get_uint64_bin(env, argv[8], &(args->old_offset)))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      khiter_t itr;
      entries_hash_t* hash;
      bitcask_keydir_entry* old_entry;
      bitcask_keydir* keydir = args->handle->keydir;
      LOCK(keydir);

      DEBUG("+++ Put file_id=%d offset=%d total_sz=%d\r\n",
            (int)args->entry.file_id, (int)args->entry.offset,
            (int)args->entry.total_sz);

      // Check for put on a new key or updating a pending tombstone
      int tombstone = 0;
      int found = find_keydir_entry(env, keydir, &args->key, &hash, &itr, &old_entry, 0);
      if (found == 1 && hash == keydir->pending && is_pending_tombstone(old_entry))
      {
        found = 0;
        tombstone = 1;
      }

      if (!found && args->old_file_id != 0)
      {
        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_ALREADY_EXISTS);
        return;
      }

      if (!found)
      {
        keydir->key_count++;
        keydir->key_bytes += args->key.size;

        // Increment live and total stats.
        update_fstats(env, keydir, args->entry.file_id, args->entry.tstamp,
                      1, 1, args->entry.total_sz, args->entry.total_sz);
        if (tombstone)
        {
          // If a pending tombstone, update to be an active entry again
          update_entry(env, keydir, old_entry, &args->entry);
        }
        else
        {
          // Add entry to the pending hash if iterating, otherwise
          // add it to the main keydir
          hash = keydir->pending == NULL ? keydir->entries : keydir->pending;
          add_entry(env, keydir, hash, &args->key, &args->entry);
        }

        if (keydir->pending != NULL)
        {
          keydir->pending_updated++;
        }
        if (args->entry.file_id > keydir->biggest_file_id)
        {
          keydir->biggest_file_id = args->entry.file_id;
        }

        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_OK);
        return;
      }

      // If old_file_id is > 0, then test-and-set fails,
      // then return already_exists.
      if (args->old_file_id != 0 &&
          !(args->old_file_id == old_entry->file_id &&
            args->old_offset == old_entry->offset))
      {
        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_ALREADY_EXISTS);
        return;
      }

      // Now that we've marshalled everything, see if the tstamp for this key is >=
      // to what's already in the hash. Otherwise, we don't bother with the update.
      if ((args->entry.newest_put &&
           (args->entry.file_id >= keydir->biggest_file_id)) ||
          (!args->entry.newest_put &&
           (old_entry->tstamp < args->entry.tstamp)) ||
          (!args->entry.newest_put &&
           ((old_entry->file_id < args->entry.file_id) ||
            (((old_entry->file_id == args->entry.file_id) &&
              (old_entry->offset < args->entry.offset))))))
      {
        // Remove the stats for the old entry and add the new
        if (old_entry->file_id != args->entry.file_id) // different files
        {
          update_fstats(env, keydir, old_entry->file_id, 0,
                        -1, 0,
                        -old_entry->total_sz, 0);
          update_fstats(env, keydir, args->entry.file_id, args->entry.tstamp,
                        1, 1,
                        args->entry.total_sz, args->entry.total_sz);
        }
        else // file_id is same, change live/total in one entry
        {
          update_fstats(env, keydir, args->entry.file_id, 0,
                        0, 1,
                        args->entry.total_sz - old_entry->total_sz,
                        args->entry.total_sz);
        }

        if (keydir->pending == NULL || // not folding
            hash == keydir->pending)   // or the old_entry already in pending
        {
          // Update the entry info. Note that if you do multiple updates in a
          // second, the last one in wins!
          // TODO: Is this safe?
          update_entry(env, keydir, old_entry, &args->entry);
        }
        else  // old_entry is in entries, add new to pending
        {
          add_entry(env, keydir, keydir->pending, &args->key, &args->entry);
        }

        if (keydir->pending != NULL)
        {
          keydir->pending_updated++;
        }
        if (args->entry.file_id > keydir->biggest_file_id)
        {
          keydir->biggest_file_id = args->entry.file_id;
        }

        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_OK);
        return;
      }
      else
      {
        // If the keydir is in the process of being loaded, it's safe to update
        // fstats on a failed put. Once the keydir is live, any attempts to put
        // in old data would just be ignored to avoid double-counting problems.
        if (!keydir->is_ready)
        {
          // Increment the total # of keys and total size for the entry that
          // was NOT stored in the keydir.
          update_fstats(env, keydir, args->entry.file_id, args->entry.tstamp,
                        0, 1, 0, args->entry.total_sz);
        }
        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_ALREADY_EXISTS);
      }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_get,
    { // struct

      bitcask_keydir_handle* handle;
      ErlNifBinary key;
    },
    { // pre

      if (!(enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle) &&
            enif_inspect_binary(env, argv[1], &args->key))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      bitcask_keydir_entry* entry = NULL;
      bitcask_keydir* keydir = args->handle->keydir;
      LOCK(keydir);

      DEBUG("+++ Get issued\r\n");

      if (find_keydir_entry(env, keydir, &args->key, NULL, NULL, &entry, args->handle->iterating) &&
          !is_pending_tombstone(entry))
      {
        ERL_NIF_TERM result = enif_make_tuple6(env,
                                               ATOM_BITCASK_ENTRY,
                                               enif_make_binary(env, &args->key), /* Key */
                                               enif_make_uint(env, entry->file_id),
                                               enif_make_uint(env, entry->total_sz),
                                               enif_make_uint64_bin(env, entry->offset),
                                               enif_make_uint(env, entry->tstamp));
        DEBUG(" ... returned value\r\n");
        UNLOCK(keydir);
        ASYNC_NIF_REPLY(result);
      }
      else
      {
        DEBUG(" ... not_found\r\n");
        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_NOT_FOUND);
      }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_remove,
    { // struct

      bitcask_keydir_handle* handle;
      ErlNifBinary key;
      uint32_t tstamp;
      uint32_t file_id;
      uint64_t offset;
    },
    { // pre

      // This can be called by bitcask_nifs:keydir_remove/2 or...
      if (!(enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle) &&
            enif_inspect_binary(env, argv[1], &args->key))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      // bitcask_nifs:keydir_remove/5
      if (argc == 5 &&
          !(enif_get_uint(env, argv[2], (unsigned int*)&args->tstamp) &&
            enif_get_uint(env, argv[3], (unsigned int*)&args->file_id) &&
            enif_get_uint64_bin(env, argv[4], (uint64_t*)&args->offset))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      khiter_t itr;
      entries_hash_t* hash;
      bitcask_keydir_entry* entry;
      bitcask_keydir* keydir = args->handle->keydir;
      LOCK(keydir);

      DEBUG("+++ Remove\r\n");

      if (find_keydir_entry(env, keydir, &args->key, &hash, &itr, &entry, 0))
      {
        // If this call has 5 arguments, this is a conditional removal. We
        // only want to actually remove the entry if the tstamp, fileid and
        // offset matches the one provided. A sort of poor-man's CAS.

        if (entry->tstamp != args->tstamp || entry->file_id != args->file_id ||
            entry->offset != args->offset)
        {
          // Either tstamp or file_id didn't match precisely. Ignore
          // this attempt to delete the record.
          UNLOCK(keydir);
          ASYNC_NIF_REPLY(ATOM_OK);
          return;
        }

        // Remove the key from the keydir stats
        keydir->key_count--;
        keydir->key_bytes -= entry->key_sz;

        // Remove from file stats
        update_fstats(env, keydir, entry->file_id, entry->tstamp,
                      -1, 0, -entry->total_sz, 0);

        // If found an entry in the entries hash and not folding, remove it
        if (keydir->pending == NULL)
        {
          remove_entry(env, keydir, itr, entry);
          enif_free(entry);
        }
        // If found an entry in the pending hash, convert it to a tombstone
        else if (keydir->pending == hash)
        {
          // If not already a tomstone, update stats and make it one
          if (!is_pending_tombstone(entry))
          {
            set_pending_tombstone(entry);
          }
        }
        // Otherwise add a tombstone to the pending hash (iteration must have
        // started between put/remove call in bitcask:delete.
        else
        {
          bitcask_keydir_entry* pending_entry =
            add_entry(env, keydir, keydir->pending, &args->key, entry);
          set_pending_tombstone(pending_entry);
        }

        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_OK);
      }
      else // entry not found, should not get here in normal operation nothing to update
      {
        UNLOCK(keydir);
        ASYNC_NIF_REPLY(ATOM_OK);
      }
    },
    { // post
      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_copy,
    { // struct

      bitcask_keydir_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle)) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      bitcask_keydir* keydir = args->handle->keydir;
      LOCK(keydir);

      // TODO: is this new_handle trickery really necessary? could it cause problems?
      bitcask_keydir_handle* new_handle;
      new_handle = enif_alloc_resource(bitcask_keydir_RESOURCE,
                                       sizeof(bitcask_keydir_handle));
      memset(args->handle, 0, sizeof(bitcask_keydir_handle));

      // Now allocate the actual keydir instance. Because it's unnamed/shared, we'll
      // leave the name and lock portions null'd out
      bitcask_keydir* new_keydir = enif_alloc(sizeof(bitcask_keydir));
      memset(new_keydir, 0, sizeof(bitcask_keydir));
      new_handle->keydir = new_keydir;
      memset(new_keydir, 0, sizeof(bitcask_keydir));
      new_keydir->entries  = kh_init(entries);
      new_keydir->fstats   = kh_init(fstats);

      // Deep copy each item from the existing handle
      khiter_t itr;
      for (itr = kh_begin(keydir->entries); itr != kh_end(keydir->entries); ++itr)
      {
        // Allocate our entry to be inserted into the new table and copy the record
        // over.
        if (kh_exist(keydir->entries, itr))
        {
          bitcask_keydir_entry* curr = kh_key(keydir->entries, itr);
          size_t new_sz = sizeof(bitcask_keydir_entry) + curr->key_sz;
          bitcask_keydir_entry* new = enif_alloc(new_sz);
          memset(new, 0, sizeof(bitcask_keydir_entry));
          memcpy(new, curr, new_sz);
          kh_put_set(entries, new_keydir->entries, new);
        }
      }
      if (keydir->pending != NULL)
      {
        for (itr = kh_begin(keydir->pending); itr != kh_end(keydir->pending); ++itr)
        {
          // Allocate our entry to be inserted into the new table and copy the record
          // over.
          if (kh_exist(keydir->pending, itr))
          {
            bitcask_keydir_entry* curr = kh_key(keydir->pending, itr);
            size_t new_sz = sizeof(bitcask_keydir_entry) + curr->key_sz;
            bitcask_keydir_entry* new = enif_alloc(new_sz);
            memset(new, 0, sizeof(bitcask_keydir_entry));
            memcpy(new, curr, new_sz);
            kh_put_set(entries, new_keydir->pending, new);
          }
        }
      }

      // Deep copy fstats info
      for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
      {
        if (kh_exist(keydir->fstats, itr))
        {
          bitcask_fstats_entry* curr_f = kh_val(keydir->fstats, itr);
          bitcask_fstats_entry* new_f = enif_alloc(sizeof(bitcask_fstats_entry));
          memset(new_f, 0, sizeof(bitcask_fstats_entry));
          memcpy(new_f, curr_f, sizeof(bitcask_fstats_entry));
          kh_put2(fstats, new_keydir->fstats, new_f->file_id, new_f);
        }
      }

      if (keydir->pending != NULL)
      {
        merge_pending_entries(env, keydir);
      }

      UNLOCK(keydir);

      ERL_NIF_TERM result = enif_make_resource(env, new_handle);
      enif_release_resource(new_handle);
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

// Helper for bitcask_nifs_keydir_itr to decide if it is valid to iterate over entries.
// Check the number of updates since pending was created is less than the maximum
// and that the current view is not too old.
// Call with ts set to zero to force a wait on any pending keydir.
// Set maxage or maxputs negative to ignore them.  Set both negative to force
// using the keydir - useful when a process has waited once and needs to run
// next time.
static int can_itr_keydir(bitcask_keydir* keydir, uint64_t ts, int maxage, int maxputs)
{
    if (keydir->pending == NULL ||   // not frozen or caller wants to reuse
        (maxage < 0 && maxputs < 0)) // the exiting freeze
    {
        return 1;
    }
    else if (ts == 0 || ts < keydir->pending_start)
    {             // if clock skew (or forced wait), force key folding to wait
        return 0; // which will fix keydir->pending_start
    }
    else
    {
        uint64_t age = ts - keydir->pending_start;
        return ((maxage < 0 || age <= maxage) &&
                (maxputs < 0 || keydir->pending_updated <= maxputs));
    }
}

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_itr,
    { // struct

      bitcask_keydir_handle* handle;
      uint64_t ts;
      int maxage;
      int maxputs;
    },
    { // pre

      if (!(enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle) &&
            enif_get_uint64_bin(env, argv[1], (uint64_t*)&args->ts) &&
            enif_get_int(env, argv[2], (int*)&args->maxage) &&
            enif_get_int(env, argv[3], (int*)&args->maxputs))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      LOCK(args->handle->keydir);
      DEBUG("+++ itr\r\n");
      bitcask_keydir* keydir = args->handle->keydir;

      // If a iterator thread is already active for this keydir, bail
      if (args->handle->iterating)
      {
        UNLOCK(args->handle->keydir);
        ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_IN_PROCESS));
        return;
      }

      if (can_itr_keydir(keydir, args->ts, args->maxage, args->maxputs))
      {
        if (keydir->pending == NULL)
        {
          keydir->pending = kh_init(entries);
          keydir->pending_start = args->ts;
        }
        args->handle->iterating = 1;
        keydir->keyfolders++;
        args->handle->iterator = kh_begin(keydir->entries);
        UNLOCK(args->handle->keydir);
        ASYNC_NIF_REPLY(ATOM_OK);
      }
      else
      {
        // Grow the pending_awaken array if necessary
        if (keydir->pending_awaken_count == keydir->pending_awaken_size)
        {   // Grow 16-at-a-time, expect a single alloc
          keydir->pending_awaken_size += 16;
          size_t size = keydir->pending_awaken_size * sizeof(keydir->pending_awaken[0]);
          if (keydir->pending_awaken == NULL)
          {
            keydir->pending_awaken = enif_alloc(size);
            memset(keydir->pending_awaken, 0, size);
          }
          else
          {
            keydir->pending_awaken = enif_realloc(keydir->pending_awaken, size);
          }
        }
        enif_self(env, &keydir->pending_awaken[keydir->pending_awaken_count]);
        keydir->pending_awaken_count++;
        UNLOCK(args->handle->keydir);
        ASYNC_NIF_REPLY(ATOM_OUT_OF_DATE);
      }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_itr_next,
    { // struct

      bitcask_keydir_handle* handle;
    },
    { // pre
      if (!(enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      bitcask_keydir* keydir = args->handle->keydir;

      if (args->handle->iterating != 1)
      {
        // Iteration not started!
        ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_NOT_STARTED));
        return;
      }

      while (args->handle->iterator != kh_end(keydir->entries))
      {
        if (kh_exist(keydir->entries, args->handle->iterator))
        {
          bitcask_keydir_entry* entry = kh_key(keydir->entries, args->handle->iterator);
          ErlNifBinary key;

          // Alloc the binary and make sure it succeeded
          if (!enif_alloc_binary(entry->key_sz, &key))
          {
            ASYNC_NIF_REPLY(ATOM_ALLOCATION_ERROR);
            return;
          }

          // Copy the data from our key to the new allocated binary
          // TODO: If we maintained a ErlNifBinary in the original entry, could we
          // get away with not doing a copy here?
          memcpy(key.data, entry->key, entry->key_sz);
          ERL_NIF_TERM curr;
          curr = enif_make_tuple6(env,
                                  ATOM_BITCASK_ENTRY,
                                  enif_make_binary(env, &key),
                                  enif_make_uint(env, entry->file_id),
                                  enif_make_uint(env, entry->total_sz),
                                  enif_make_uint64_bin(env, entry->offset),
                                  enif_make_uint(env, entry->tstamp));

          // Update the iterator to the next entry
          (args->handle->iterator)++;
          ASYNC_NIF_REPLY(curr);
          return;
        }
        else
        {
          // No item in this slot; increment the iterator and keep looping
          (args->handle->iterator)++;
        }
      }

      // The iterator is at the end of the table
      ASYNC_NIF_REPLY(ATOM_NOT_FOUND);
    },
    { // post
      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_itr_release,
    { // struct

      bitcask_keydir_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle)) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      LOCK(args->handle->keydir);
      if (args->handle->iterating != 1)
      {
            // Iteration not started!
        UNLOCK(args->handle->keydir);
        ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_NOT_STARTED));
        return;
      }

      args->handle->iterating = 0;
      args->handle->keydir->keyfolders--;
      if (args->handle->keydir->keyfolders == 0)
      {
        merge_pending_entries(env, args->handle->keydir);
        args->handle->keydir->iter_generation++;
      }
      UNLOCK(args->handle->keydir);
      ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_info,
    { // struct

      bitcask_keydir_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle)) {
        ASYNC_NIF_RETURN_BADARG();
      }
      if (args->handle->keydir == NULL) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      bitcask_keydir* keydir = args->handle->keydir;
      LOCK(keydir);

      // Dump fstats info into a list of [{file_id, live_keys, total_keys,
      //                                   live_bytes, total_bytes,
      //                                   oldest_tstamp, newest_tstamp}]
      ERL_NIF_TERM fstats_list = enif_make_list(env, 0);
      khiter_t itr;
      bitcask_fstats_entry* curr_f;
      for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
      {
        if (kh_exist(keydir->fstats, itr))
        {
          curr_f = kh_val(keydir->fstats, itr);
          ERL_NIF_TERM fstat =
            enif_make_tuple7(env,
                             enif_make_uint(env, curr_f->file_id),
                             enif_make_ulong(env, curr_f->live_keys),
                             enif_make_ulong(env, curr_f->total_keys),
                             enif_make_ulong(env, curr_f->live_bytes),
                             enif_make_ulong(env, curr_f->total_bytes),
                             enif_make_uint(env, curr_f->oldest_tstamp),
                             enif_make_uint(env, curr_f->newest_tstamp));
          fstats_list = enif_make_list_cell(env, fstat, fstats_list);
        }
      }

      ERL_NIF_TERM iter_info =
        enif_make_tuple3(env,
                         enif_make_uint64_bin(env, keydir->iter_generation),
                         enif_make_ulong(env, keydir->keyfolders),
                         keydir->pending == NULL ? ATOM_FALSE : ATOM_TRUE);
      ERL_NIF_TERM result = enif_make_tuple4(env,
                                             enif_make_ulong(env, keydir->key_count),
                                             enif_make_ulong(env, keydir->key_bytes),
                                             fstats_list,
                                             iter_info);
      UNLOCK(keydir);
      ASYNC_NIF_REPLY(result);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_keydir_release,
    { // struct

      bitcask_keydir_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&args->handle)) {
        ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

      bitcask_nifs_keydir_priv_resource_cleanup(env, args->handle, priv_data);
      ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_lock_acquire,
    { // struct

        ERL_NIF_TERM ref;
        char filename[4096];
        int is_write_lock;
    },
    { // pre

        args->is_write_lock = 0;
        if (!((enif_get_string(env, argv[0], args->filename, sizeof(args->filename), ERL_NIF_LATIN1) > 0) &&
              enif_get_int(env, argv[1], &(args->is_write_lock)))) {
          ASYNC_NIF_RETURN_BADARG();
        }
        args->ref = argv[0];
    },
    { // work

        // Setup the flags for the lock file
        int flags = O_RDONLY;
        if (args->is_write_lock)
        {
            // Use O_SYNC (in addition to other flags) to ensure that when we write
            // data to the lock file it is immediately (or nearly) available to any
            // other reading processes
            flags = O_CREAT | O_EXCL | O_RDWR | O_SYNC;
        }

        // Try to open the lock file -- allocate a resource if all goes well.
        int fd = open(args->filename, flags, 0600);
        if (fd > -1)
        {
            // Successfully opened the file -- setup a resource to track the FD.
            unsigned int filename_sz = strlen(args->filename) + 1;
            bitcask_lock_handle* handle;
            handle = enif_alloc_resource(bitcask_lock_RESOURCE,
                                         sizeof(bitcask_lock_handle) +
                                         filename_sz);
            memset(handle, 0, sizeof(bitcask_lock_handle));
            handle->fd = fd;
            handle->is_write_lock = args->is_write_lock;
            strncpy(handle->filename, args->filename, filename_sz);
            ERL_NIF_TERM result = enif_make_resource(env, handle);
            enif_release_resource(handle);

            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
        }
        else
        {
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
        }
    },
    { // post
    });

ASYNC_NIF_DECL(
    bitcask_nifs_lock_release,
    { // struct

        bitcask_lock_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_lock_RESOURCE, (void**)&(args->handle))) {
            ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        lock_release(args->handle);
        ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_lock_readdata,
    { // struct

        bitcask_lock_handle* handle;
    },
    { // pre

        if (!enif_get_resource(env, argv[0], bitcask_lock_RESOURCE, (void**)&(args->handle))) {
            ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        // Stat the filehandle so we can read the entire contents into memory
        struct stat sinfo;
        if (fstat(args->handle->fd, &sinfo) != 0)
        {
            ASYNC_NIF_REPLY(errno_error_tuple(env, ATOM_FSTAT_ERROR, errno));
            return;
        }

        // Allocate a binary to hold the contents of the file
        ErlNifBinary data;
        if (!enif_alloc_binary(sinfo.st_size, &data))
        {
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR));
            return;
        }

        // Read the whole file into our binary
        if (pread(args->handle->fd, data.data, data.size, 0) == -1)
        {
            ASYNC_NIF_REPLY(errno_error_tuple(env, ATOM_PREAD_ERROR, errno));
            return;
        }

        ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &data)));
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_lock_writedata,
    { // struct

        bitcask_lock_handle* handle;
        ErlNifBinary data;
    },
    { // pre

        if (!(enif_get_resource(env, argv[0], bitcask_lock_RESOURCE, (void**)&(args->handle)) &&
              enif_inspect_binary(env, argv[1], &(args->data)))) {
          ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        if (args->handle->is_write_lock)
        {
            // Truncate the file first, to ensure that the lock file only contains what
            // we're about to write
            if (ftruncate(args->handle->fd, 0) == -1)
            {
                ASYNC_NIF_REPLY(errno_error_tuple(env, ATOM_FTRUNCATE_ERROR, errno));
                return;
            }

            // Write the new blob of data to the lock file. Note that we use O_SYNC to
            // ensure that the data is available ASAP to reading processes.
            if (pwrite(args->handle->fd, args->data.data, args->data.size, 0) == -1)
            {
                ASYNC_NIF_REPLY(errno_error_tuple(env, ATOM_PWRITE_ERROR, errno));
                return;
            }

            ASYNC_NIF_REPLY(ATOM_OK);
        }
        else
        {
            // Tried to write data to a read lock
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_LOCK_NOT_WRITABLE));
        }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

int get_file_open_flags(ErlNifEnv* env, ERL_NIF_TERM list)
{
    int flags = -1;
    ERL_NIF_TERM head, tail;
    while (enif_get_list_cell(env, list, &head, &tail))
    {
        if (head == ATOM_CREATE)
        {
            flags = O_CREAT | O_EXCL | O_RDWR | O_APPEND;
        }
        else if (head == ATOM_READONLY)
        {
            flags = O_RDONLY;
        }
        else if (head == ATOM_O_SYNC)
        {
            flags |= O_SYNC;
        }

        list = tail;
    }
    return flags;
}


ASYNC_NIF_DECL(
    bitcask_nifs_file_open,
    { // struct

        char filename[4096];
        ERL_NIF_TERM oflags;
    },
    { // pre

      if (!((enif_get_string(env, argv[0], args->filename, sizeof(args->filename), ERL_NIF_LATIN1) > 0) &&
            enif_is_list(env, argv[1]))) {
        ASYNC_NIF_RETURN_BADARG();
      }
      args->oflags = argv[1];
    },
    { // work

        int flags = get_file_open_flags(env, args->oflags);
        int fd = open(args->filename, flags, S_IREAD | S_IWRITE);
        if (fd > -1)
        {
            // Setup a resource for our handle
          bitcask_file_handle* handle;
          handle = enif_alloc_resource(bitcask_file_RESOURCE,
                                       sizeof(bitcask_file_handle));
          memset(handle, 0, sizeof(bitcask_file_handle));
          handle->fd = fd;

          ERL_NIF_TERM result = enif_make_resource(env, handle);
          enif_release_resource(handle);
          ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
        }
        else
        {
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
        }
    },
    { // post
    })

ASYNC_NIF_DECL(
    bitcask_nifs_file_close,
    { // struct

        bitcask_file_handle* handle;
    },
    { // pre

      if (!enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle))) {
          ASYNC_NIF_RETURN_BADARG();
      }
      enif_keep_resource((void*)args->handle);
    },
    { // work

        if (args->handle->fd > 0)
        {
            /* TODO: Check for EIO */
            close(args->handle->fd);
            args->handle->fd = -1;
        }
        ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_file_sync,
    { // struct

        bitcask_file_handle* handle;
    },
    { // pre

        if (!enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle))){
            ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        int rc = fsync(args->handle->fd);
        if (rc != -1)
        {
            ASYNC_NIF_REPLY(ATOM_OK);
        }
        else
        {
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
        }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_file_pread,
    { // struct

        bitcask_file_handle* handle;
        unsigned long offset_ul;
        unsigned long count_ul;
    },
    { // pre

        if (!(enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle)) &&
              enif_get_ulong(env, argv[1], &(args->offset_ul)) && /* Offset */
              enif_get_ulong(env, argv[2], &(args->count_ul)))) {  /* Count */
          ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        ErlNifBinary bin;
        off_t offset = args->offset_ul;
        size_t count = args->count_ul;
        if (!enif_alloc_binary(count, &bin))
        {
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR));
            return;
        }

        ssize_t bytes_read = pread(args->handle->fd, bin.data, count, offset);
        if (bytes_read == count)
        {
            /* Good read; return {ok, Bin} */
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin)));
        }
        else if (bytes_read > 0)
        {
            /* Partial read; need to resize our binary (bleh) and return {ok, Bin} */
            if (enif_realloc_binary(&bin, bytes_read))
            {
                ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin)));
            }
            else
            {
                /* Realloc failed; cleanup and bail */
                enif_release_binary(&bin);
                ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR));
            }
        }
        else if (bytes_read == 0)
        {
            /* EOF */
            enif_release_binary(&bin);
            ASYNC_NIF_REPLY(ATOM_EOF);
        }
        else
        {
            /* Read failed altogether */
            enif_release_binary(&bin);
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
        }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_file_pwrite,
    { // struct

        bitcask_file_handle* handle;
        unsigned long offset_ul;
        ErlNifBinary bin;
    },
    { // pre

        if (!(enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle)) &&
              enif_get_ulong(env, argv[1], &(args->offset_ul)) && /* Offset */
              enif_inspect_iolist_as_binary(env, argv[2], &(args->bin)))) {  /* Bytes to write */
          ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        unsigned char* buf = args->bin.data;
        ssize_t bytes_written = 0;
        ssize_t count = args->bin.size;
        off_t offset = args->offset_ul;

        while (count > 0)
        {
            bytes_written = pwrite(args->handle->fd, buf, count, offset);
            if (bytes_written > 0)
            {
                count -= bytes_written;
                offset += bytes_written;
                buf += bytes_written;
            }
            else
            {
                /* Write failed altogether */
                ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
                return;
            }
        }

        /* Write done */
        ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_file_read,
    { // struct

        bitcask_file_handle* handle;
        size_t count;
    },
    { // pre

        if (!(enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle)) &&
              enif_get_ulong(env, argv[1], &(args->count)))) { /* Count */
          ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        ErlNifBinary bin;
        if (!enif_alloc_binary(args->count, &bin))
        {
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR));
            return;
        }

        ssize_t bytes_read = read(args->handle->fd, bin.data, args->count);
        if (bytes_read == args->count)
        {
            /* Good read; return {ok, Bin} */
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin)));
        }
        else if (bytes_read > 0)
        {
            /* Partial read; need to resize our binary (bleh) and return {ok, Bin} */
            if (enif_realloc_binary(&bin, bytes_read))
            {
                ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin)));
            }
            else
            {
                /* Realloc failed; cleanup and bail */
                enif_release_binary(&bin);
                ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR));
            }
        }
        else if (bytes_read == 0)
        {
            /* EOF */
            enif_release_binary(&bin);
            ASYNC_NIF_REPLY(ATOM_EOF);
        }
        else
        {
            /* Read failed altogether */
            enif_release_binary(&bin);
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
        }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_file_write,
    { // struct

        bitcask_file_handle* handle;
        ErlNifBinary bin;
    },
    { // pre

        if (!(enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle)) &&
              enif_inspect_iolist_as_binary(env, argv[1], &(args->bin)))) { /* Bytes to write */
          ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        unsigned char* buf = args->bin.data;
        ssize_t bytes_written = 0;
        ssize_t count = args->bin.size;
        while (count > 0)
        {
            bytes_written = write(args->handle->fd, buf, count);
            if (bytes_written > 0)
            {
                count -= bytes_written;
                buf += bytes_written;
            }
            else
            {
                /* Write failed altogether */
                ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
                return;
            }
        }

        /* Write done */
        ASYNC_NIF_REPLY(ATOM_OK);
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ASYNC_NIF_DECL(
    bitcask_nifs_file_seekbof,
    { // struct

      bitcask_file_handle* handle;
    },
    { // pre

        if (!enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&(args->handle))) {
            ASYNC_NIF_RETURN_BADARG();
        }
        enif_keep_resource((void*)args->handle);
    },
    { // work

        if(lseek(args->handle->fd, 0, SEEK_SET) != -1) {
            ASYNC_NIF_REPLY(ATOM_OK);
        }
        else {
            /* Write failed altogether */
            ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno)));
        }
    },
    { // post

      enif_release_resource((void*)args->handle);
    });

ERL_NIF_TERM errno_atom(ErlNifEnv* env, int error)
{
    return enif_make_atom(env, erl_errno_id(error));
}

ERL_NIF_TERM errno_error_tuple(ErlNifEnv* env, ERL_NIF_TERM key, int error)
{
    // Construct a tuple of form: {error, {Key, ErrnoAtom}}
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_tuple2(env, key, errno_atom(env, error)));
}

// Send messages to all processes that want to be awoken next time
// pending is merged.
static void msg_pending_awaken(ErlNifEnv* env, bitcask_keydir* keydir,
                               ERL_NIF_TERM msg)
{
    ErlNifEnv* msg_env = enif_alloc_env();
    int idx;
    for (idx = 0; idx < keydir->pending_awaken_count; idx++)
    {
        enif_clear_env(msg_env);
#ifdef PULSE
        PULSE_SEND(env, &keydir->pending_awaken[idx], msg_env, msg);
#else
        enif_send(env, &keydir->pending_awaken[idx], msg_env, msg);
#endif
    }
    enif_free_env(msg_env);
}

// Merge pending hash into entries hash and awaken any pids that want to
// start iterating once we are merged.  keydir must be locked before calling.
static void merge_pending_entries(ErlNifEnv* env, bitcask_keydir* keydir)
{
    khiter_t pend_itr;
    for (pend_itr = kh_begin(keydir->pending); pend_itr != kh_end(keydir->pending); ++pend_itr)
    {
        if (kh_exist(keydir->pending, pend_itr))
        {
            bitcask_keydir_entry* pending_entry = kh_key(keydir->pending, pend_itr);
            khiter_t ent_itr = kh_get(entries, keydir->entries, pending_entry);

            DEBUG("Pending Entry: key=%s key_sz=%d file_id=%d tstamp=%u offset=%u size=%d\r\n",
                    pending_entry->key, pending_entry->key_sz,
                    pending_entry->file_id,
                    (unsigned int) pending_entry->tstamp,
                    (unsigned int) pending_entry->offset,
                    pending_entry->total_sz);

            if (ent_itr == kh_end(keydir->entries))
            {
                /* entries: empty, pending:tombstone */
                if (is_pending_tombstone(pending_entry))
                {
                    /* nop - stats were not updated when tombstone written for
                    ** empty entry
                    */
                    enif_free(pending_entry);
                }
                /* entries: empty, pending:value */
                else
                {
                    move_pending_entry(env, keydir, pend_itr, pending_entry);
                    // do not free - now in entries
                }
            }
            else
            {
                bitcask_keydir_entry* entries_entry = kh_key(keydir->entries, ent_itr);
                DEBUG("Entries Entry: key=%s key_sz=%d file_id=%d statmp=%u offset=%u size=%d\r\n",
                        entries_entry->key, entries_entry->key_sz,
                        entries_entry->file_id,
                        (unsigned int) entries_entry->tstamp,
                        (unsigned int) entries_entry->offset,
                        entries_entry->total_sz);

                /* entries: present, pending:tombstone */
                if (is_pending_tombstone(pending_entry))
                {
                    remove_entry(env, keydir, ent_itr, entries_entry);
                    enif_free(entries_entry);
                }
                /* entries: present, pending:value */
                else
                {
                    update_entry(env, keydir, entries_entry, pending_entry);
                }
                enif_free(pending_entry);
            }
        }
    }

    // Wake up all sleeping pids
    msg_pending_awaken(env, keydir, ATOM_READY);

    // Free all resources for keydir folding
    kh_destroy(entries, keydir->pending);
    keydir->pending = NULL;

    keydir->pending_updated = 0;
    keydir->pending_start = 0;
    if (keydir->pending_awaken != NULL)
    {
        enif_free(keydir->pending_awaken);
    }
    keydir->pending_awaken = NULL;
    keydir->pending_awaken_count = 0;
    keydir->pending_awaken_size = 0;

    DEBUG("Merge pending entries completed\r\n");
}


static void lock_release(bitcask_lock_handle* handle)
{
    if (handle->fd > 0)
    {
        // If this is a write lock, we need to delete the file as part of cleanup. But be
        // sure to do this BEFORE letting go of the file handle so as to ensure consistency
        // with other readers.
        if (handle->is_write_lock)
        {
            // TODO: Come up with some way to complain/error log if this unlink failed for some
            // reason!!
            unlink(handle->filename);
        }

        close(handle->fd);
        handle->fd = -1;
    }
}

static void free_keydir(ErlNifEnv* env, bitcask_keydir* keydir)
{
    // Delete all the entries in the hash table, which also has the effect of
    // freeing up all resources associated with the table.
    khiter_t itr;
    bitcask_keydir_entry* current_entry;
    for (itr = kh_begin(keydir->entries); itr != kh_end(keydir->entries); ++itr)
    {
        if (kh_exist(keydir->entries, itr))
        {
            current_entry = kh_key(keydir->entries, itr);
            enif_free(current_entry);
        }
    }

    kh_destroy(entries, keydir->entries);

    bitcask_fstats_entry* curr_f;

    for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
    {
        if (kh_exist(keydir->fstats, itr))
        {
            curr_f = kh_val(keydir->fstats, itr);
            enif_free(curr_f);
        }
    }

    kh_destroy(fstats, keydir->fstats);
}


static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg)
{
    bitcask_keydir_handle* handle = (bitcask_keydir_handle*)arg;
    bitcask_priv_data* priv = (bitcask_priv_data*)enif_priv_data(env);
    bitcask_nifs_keydir_priv_resource_cleanup(env, handle, priv);
}

static void bitcask_nifs_keydir_priv_resource_cleanup(ErlNifEnv* env, bitcask_keydir_handle *handle, bitcask_priv_data* priv)
{
    bitcask_keydir* keydir = handle->keydir;

    // First, check that there is even a keydir available. If keydir_release
    // was invoked manually, we might have already cleaned up the keydir
    // and this round of cleanup can noop. Otherwise, clear out the handle's
    // reference to the keydir so that repeat calls function as expected
    if (!handle->keydir)
    {
        return;
    }
    else
    {
        handle->keydir = 0;
    }

    // If the keydir has a lock, we need to decrement the refcount and
    // potentially release it
    if (keydir->mutex)
    {
        enif_mutex_lock(priv->global_keydirs_lock);
        keydir->refcount--;
        if (keydir->refcount == 0)
        {
            // This is the last reference to the named keydir. As such,
            // remove it from the hashtable so no one else tries to use it
            khiter_t itr = kh_get(global_keydirs, priv->global_keydirs, keydir->name);
            kh_del(global_keydirs, priv->global_keydirs, itr);
        }
        else
        {
            // At least one other reference; just throw away our keydir pointer
            // so the check below doesn't release the memory.
            keydir = 0;
        }

        // Unlock ASAP. Wanted to avoid holding this mutex while we clean up the
        // keydir, since it may take a while to walk a large keydir and free each
        // entry.
        enif_mutex_unlock(priv->global_keydirs_lock);
    }

    // If keydir is still defined, it's either privately owner or has a
    // refcount of 0. Either way, we want to release it.
    if (keydir)
    {
        if (keydir->mutex)
        {
            enif_mutex_destroy(keydir->mutex);
        }

        free_keydir(env, keydir);
    }
}

static void bitcask_nifs_lock_resource_cleanup(ErlNifEnv* env, void* arg)
{
    bitcask_lock_handle* handle = (bitcask_lock_handle*)arg;
    lock_release(handle);
}

static void bitcask_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg)
{
    bitcask_file_handle* handle = (bitcask_file_handle*)arg;
    if (handle->fd > -1)
    {
        close(handle->fd);
    }
}


#ifdef BITCASK_DEBUG
static void dump_fstats(bitcask_keydir* keydir)
{
    bitcask_fstats_entry* curr_f;
    khiter_t itr;
    for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
    {
        if (kh_exist(keydir->fstats, itr))
        {
            curr_f = kh_val(keydir->fstats, itr);
            DEBUG("fstats %d live=(%d,%d) total=(%d,%d)\r\n",
                    (int) curr_f->file_id,
                    (int) curr_f->live_keys,
                    (int) curr_f->live_bytes,
                    (int) curr_f->total_keys,
                    (int) curr_f->total_bytes);
        }
    }
}
#endif

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ASYNC_NIF_LOAD();
    bitcask_keydir_RESOURCE = enif_open_resource_type(env, NULL, "bitcask_keydir_resource",
                                                      &bitcask_nifs_keydir_resource_cleanup,
                                                      ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                      0);

    bitcask_lock_RESOURCE = enif_open_resource_type(env, NULL, "bitcask_lock_resource",
                                                    &bitcask_nifs_lock_resource_cleanup,
                                                    ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                    0);

    bitcask_file_RESOURCE = enif_open_resource_type(env, NULL, "bitcask_file_resource",
                                                    &bitcask_nifs_file_resource_cleanup,
                                                    ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                    0);

    // Initialize shared keydir hashtable
    bitcask_priv_data* priv = enif_alloc(sizeof(bitcask_priv_data));
    memset(priv, 0, sizeof(bitcask_priv_data));
    priv->global_keydirs = kh_init(global_keydirs);
    priv->global_keydirs_lock = enif_mutex_create("bitcask_global_handles_lock");
    *priv_data = priv;

#ifdef PULSE
    pulse_c_send_on_load(env);
#endif
    return 0;
}

static void on_unload(ErlNifEnv* env, void* priv_data){
  // TODO: what about priv_data? seems it should be cleaned up...
  ASYNC_NIF_UNLOAD()
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
  // TODO: what about priv_data? seems it should be cleaned up...
  ASYNC_NIF_UPGRADE()
  return 0;
}

static ErlNifFunc nif_funcs[] =
{
#ifdef PULSE
    {"set_pulse_pid", 1, set_pulse_pid},
#endif
    {"keydir_new_nif", 1, bitcask_nifs_keydir_new0},
    {"keydir_new_nif", 2, bitcask_nifs_keydir_new1},
    {"keydir_mark_ready_nif", 2, bitcask_nifs_keydir_mark_ready},
    {"keydir_put_nif", 10, bitcask_nifs_keydir_put},
    {"keydir_get_nif", 3, bitcask_nifs_keydir_get},
    {"keydir_remove_nif", 3, bitcask_nifs_keydir_remove},
    {"keydir_remove_nif", 6, bitcask_nifs_keydir_remove},
    {"keydir_copy_nif", 2, bitcask_nifs_keydir_copy},
    {"keydir_itr_nif", 5, bitcask_nifs_keydir_itr},
    {"keydir_itr_next_nif", 2, bitcask_nifs_keydir_itr_next},
    {"keydir_itr_release_nif", 2, bitcask_nifs_keydir_itr_release},
    {"keydir_info_nif", 2, bitcask_nifs_keydir_info},
    {"keydir_release_nif", 2, bitcask_nifs_keydir_release},
    {"lock_acquire_nif",   3, bitcask_nifs_lock_acquire},
    {"lock_release_nif",   2, bitcask_nifs_lock_release},
    {"lock_readdata_nif",  2, bitcask_nifs_lock_readdata},
    {"lock_writedata_nif", 3, bitcask_nifs_lock_writedata},
    {"file_open_nif",   3, bitcask_nifs_file_open},
    {"file_close_nif",  2, bitcask_nifs_file_close},
    {"file_sync_nif",   2, bitcask_nifs_file_sync},
    {"file_pread_nif",  4, bitcask_nifs_file_pread},
    {"file_pwrite_nif", 4, bitcask_nifs_file_pwrite},
    {"file_read_nif",   3, bitcask_nifs_file_read},
    {"file_write_nif",  3, bitcask_nifs_file_write},
    {"file_seekbof_nif", 2, bitcask_nifs_file_seekbof}
};

ERL_NIF_INIT(bitcask_nifs, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload);
