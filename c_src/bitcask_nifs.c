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
#include <pthread.h>
#include <time.h>

#include "erl_nif.h"
#include "erl_driver.h"
#include "erl_nif_compat.h"
#include "erl_nif_util.h"

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

struct bitcask_keydir_sib
{
    uint32_t file_id;
    uint32_t total_sz;
    uint64_t offset;
    uint32_t tstamp;
    uint32_t newest_put;
    struct bitcask_keydir_sib * next;
};
typedef struct bitcask_keydir_sib bitcask_keydir_sib;


typedef struct
{
    bitcask_keydir_sib * sibs;
    uint16_t key_sz;
    char     key[0];
} bitcask_keydir_parent;

#define IS_ENTRY_LIST(p) ((uint64_t)p&1) 
#define GET_ENTRY_LIST_POINTER(p) ((bitcask_keydir_parent*)((uint64_t)p&~1))
#define MAKE_ENTRY_LIST_POINTER(p) ((bitcask_keydir_entry*)((uint64_t)p|1))

static khint_t keydir_entry_hash(bitcask_keydir_entry* entry);
static khint_t keydir_entry_equal(bitcask_keydir_entry* lhs,
                                  bitcask_keydir_entry* rhs);
KHASH_INIT(entries, bitcask_keydir_entry*, char, 0, keydir_entry_hash, keydir_entry_equal);

typedef struct
{
    uint32_t file_id;
    uint64_t live_keys;   // number of 'live' keys in entries
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
    fstats_hash_t*  fstats;
    size_t        key_count;
    size_t        key_bytes;
    uint32_t      biggest_file_id;
    unsigned int  refcount;
    unsigned int  keyfolders;
    uint32_t      newest_folder;
    uint64_t      iter_generation;
    ErlNifMutex*  mutex;
    //be nice to have this be dynamic
    //pthread_rwlock_t*  mutices[32];
    char          is_ready;
    char          name[0];

} bitcask_keydir;

typedef struct
{
    bitcask_keydir* keydir;
    int             iterating;
    khiter_t        iterator;
    uint32_t        ts;
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

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ALLOCATION_ERROR;
static ERL_NIF_TERM ATOM_ALREADY_EXISTS;
static ERL_NIF_TERM ATOM_BITCASK_ENTRY;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_FSTAT_ERROR;
static ERL_NIF_TERM ATOM_FTRUNCATE_ERROR;
static ERL_NIF_TERM ATOM_GETFL_ERROR;
static ERL_NIF_TERM ATOM_ILT_CREATE_ERROR; /* Iteration lock thread creation error */
static ERL_NIF_TERM ATOM_ITERATION_NOT_STARTED;
static ERL_NIF_TERM ATOM_LOCK_NOT_WRITABLE;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_NOT_READY;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_OUT_OF_DATE;
static ERL_NIF_TERM ATOM_PREAD_ERROR;
static ERL_NIF_TERM ATOM_PWRITE_ERROR;
static ERL_NIF_TERM ATOM_READY;
static ERL_NIF_TERM ATOM_SETFL_ERROR;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_EOF;
static ERL_NIF_TERM ATOM_CREATE;
static ERL_NIF_TERM ATOM_READONLY;
static ERL_NIF_TERM ATOM_O_SYNC;

// Prototypes
ERL_NIF_TERM bitcask_nifs_keydir_new0(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_new1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_mark_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_get_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_put_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_copy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM bitcask_nifs_create_tmp_file(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM bitcask_nifs_lock_acquire(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_lock_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_lock_readdata(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_lock_writedata(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM bitcask_nifs_file_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_pread(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_pwrite(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_file_seekbof(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM errno_atom(ErlNifEnv* env, int error);
ERL_NIF_TERM errno_error_tuple(ErlNifEnv* env, ERL_NIF_TERM key, int error);

static void lock_release(bitcask_lock_handle* handle);

//entry list stuff
static int get_entry(ErlNifEnv* env, bitcask_keydir_entry* src, 
                     bitcask_keydir_entry** dest, uint32_t tstamp);
static bitcask_keydir_sib* make_sib_tombstone(ErlNifEnv * env, uint32_t ts);
static void free_entry_list(ErlNifEnv* env, bitcask_keydir_parent* head);
static bitcask_keydir_parent* make_entry_list(ErlNifEnv * env, 
                                              bitcask_keydir_entry* entry);
static bitcask_keydir_entry* make_synthetic_entry(ErlNifEnv * env,
                                                  bitcask_keydir_parent *p,
                                                  uint32_t tstamp);

static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg);
static void bitcask_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg);

static ErlNifFunc nif_funcs[] =
{
#ifdef PULSE
    {"set_pulse_pid", 1, set_pulse_pid},
#endif
    {"keydir_new", 0, bitcask_nifs_keydir_new0},
    {"keydir_new", 1, bitcask_nifs_keydir_new1},
    {"keydir_mark_ready", 1, bitcask_nifs_keydir_mark_ready},
    {"keydir_put_int", 9, bitcask_nifs_keydir_put_int},
    {"keydir_get_int", 3, bitcask_nifs_keydir_get_int},
    {"keydir_remove", 2, bitcask_nifs_keydir_remove},
    {"keydir_remove_int", 5, bitcask_nifs_keydir_remove},
    {"keydir_copy", 1, bitcask_nifs_keydir_copy},
    {"keydir_itr_int", 4, bitcask_nifs_keydir_itr},
    {"keydir_itr_next_int", 1, bitcask_nifs_keydir_itr_next},
    {"keydir_itr_release", 1, bitcask_nifs_keydir_itr_release},
    {"keydir_info", 1, bitcask_nifs_keydir_info},
    {"keydir_release", 1, bitcask_nifs_keydir_release},

    {"lock_acquire_int",   2, bitcask_nifs_lock_acquire},
    {"lock_release_int",   1, bitcask_nifs_lock_release},
    {"lock_readdata_int",  1, bitcask_nifs_lock_readdata},
    {"lock_writedata_int", 2, bitcask_nifs_lock_writedata},

    {"file_open_int",   2, bitcask_nifs_file_open},
    {"file_close_int",  1, bitcask_nifs_file_close},
    {"file_sync_int",   1, bitcask_nifs_file_sync},
    {"file_pread_int",  3, bitcask_nifs_file_pread},
    {"file_pwrite_int", 3, bitcask_nifs_file_pwrite},
    {"file_read_int",   2, bitcask_nifs_file_read},
    {"file_write_int",  2, bitcask_nifs_file_write},
    {"file_position_int",  2, bitcask_nifs_file_position},
    {"file_seekbof_int", 1, bitcask_nifs_file_seekbof}
};

ERL_NIF_TERM bitcask_nifs_keydir_new0(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // First, setup a resource for our handle
    bitcask_keydir_handle* handle = enif_alloc_resource_compat(env,
                                                        bitcask_keydir_RESOURCE,
                                                        sizeof(bitcask_keydir_handle));
    memset(handle, '\0', sizeof(bitcask_keydir_handle));

    // Now allocate the actual keydir instance. Because it's unnamed/shared, we'll
    // leave the name and lock portions null'd out
    bitcask_keydir* keydir = enif_alloc_compat(env, sizeof(bitcask_keydir));
    memset(keydir, '\0', sizeof(bitcask_keydir));
    keydir->entries  = kh_init(entries);
    keydir->fstats   = kh_init(fstats);

    // Assign the keydir to our handle and hand it back
    handle->keydir = keydir;
    ERL_NIF_TERM result = enif_make_resource(env, handle);
    enif_release_resource_compat(env, handle);
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM bitcask_nifs_keydir_new1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name[4096];
    size_t name_sz;
    if (enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1))
    {
        name_sz = strlen(name);

        // Get our private stash and check the global hash table for this entry
        bitcask_priv_data* priv = (bitcask_priv_data*)enif_priv_data(env);
        enif_mutex_lock(priv->global_keydirs_lock);

        bitcask_keydir* keydir;
        khiter_t itr = kh_get(global_keydirs, priv->global_keydirs, name);
        if (itr != kh_end(priv->global_keydirs))
        {
            keydir = kh_val(priv->global_keydirs, itr);
            // Existing keydir is available. Check the is_ready flag to determine if
            // the original creator is ready for other processes to use it.
            if (!keydir->is_ready)
            {
                // Notify the caller that while the requested keydir exists, it's not
                // ready for public usage.
                enif_mutex_unlock(priv->global_keydirs_lock);
                return enif_make_tuple2(env, ATOM_ERROR, ATOM_NOT_READY);
            }
            else
            {
                keydir->refcount++;
            }
        }
        else
        {
            // No such keydir, create a new one and add to the globals list. Make sure
            // to allocate enough room for the name.
            keydir = enif_alloc_compat(env, sizeof(bitcask_keydir) + name_sz + 1);
            memset(keydir, '\0', sizeof(bitcask_keydir) + name_sz + 1);
            strncpy(keydir->name, name, name_sz + 1);

            // Initialize hash tables
            keydir->entries  = kh_init(entries);
            keydir->fstats   = kh_init(fstats);

            // Be sure to initialize the mutex and set our refcount
            keydir->mutex = enif_mutex_create(name);
            keydir->refcount = 1;
            keydir->keyfolders = 0;

            // Finally, register this new keydir in the globals
            kh_put2(global_keydirs, priv->global_keydirs, keydir->name, keydir);
        }

        enif_mutex_unlock(priv->global_keydirs_lock);

        // Setup a resource for the handle
        bitcask_keydir_handle* handle = enif_alloc_resource_compat(env,
                                                            bitcask_keydir_RESOURCE,
                                                            sizeof(bitcask_keydir_handle));
        memset(handle, '\0', sizeof(bitcask_keydir_handle));
        handle->keydir = keydir;
        ERL_NIF_TERM result = enif_make_resource(env, handle);
        enif_release_resource_compat(env, handle);

        // Return to the caller a tuple with the reference and an atom
        // indicating if the keydir is ready or not.
        ERL_NIF_TERM is_ready_atom = keydir->is_ready ? ATOM_READY : ATOM_NOT_READY;
        return enif_make_tuple2(env, is_ready_atom, result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_mark_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    
    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, 
                          (void**)&handle))
    {
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);
        keydir->is_ready = 1;
        UNLOCK(keydir);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

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
        entry = enif_alloc_compat(env, sizeof(bitcask_fstats_entry));
        memset(entry, '\0', sizeof(bitcask_fstats_entry));
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
    khint_t h;
    if (IS_ENTRY_LIST(entry)) 
    {
        bitcask_keydir_parent* par = GET_ENTRY_LIST_POINTER(entry); 
        h = MURMUR_HASH(par->key, par->key_sz, 42);
    }
    else
    {
        h = MURMUR_HASH(entry->key, entry->key_sz, 42);
    }
    return h;
}

static khint_t keydir_entry_equal(bitcask_keydir_entry* lhs0,
                                  bitcask_keydir_entry* rhs0)
{
    bitcask_keydir_entry *lhs, *rhs;
    int lhs_synthetic = 0;
    int rhs_synthetic = 0;
    time_t now = 0xffffffff;
    if (IS_ENTRY_LIST(lhs0)) {
        //this is not safe on older erlangs; do we care?
        lhs = make_synthetic_entry(NULL, GET_ENTRY_LIST_POINTER(lhs0),
                                   now);
        lhs_synthetic = 1;
    }
    else
    {
        lhs = lhs0;
    }

    if (IS_ENTRY_LIST(rhs0)) {
        //this is not safe on older erlangs; do we care?
        rhs = make_synthetic_entry(NULL, GET_ENTRY_LIST_POINTER(rhs0),
                                   now);
        rhs_synthetic = 1;
    }
    else
    {
        rhs = rhs0;
    }

    int ret;
    if (lhs->key_sz != rhs->key_sz)
    {
        ret = 0;
    }
    else
    {
        ret = (memcmp(lhs->key, rhs->key, lhs->key_sz) == 0);
    }

    if (lhs_synthetic) {
        enif_free_compat(NULL, lhs);
    }
    if (rhs_synthetic) {
        enif_free_compat(NULL, rhs);
    }
    return ret;
}

static khiter_t get_entries_hash(ErlNifEnv* env,
                                 entries_hash_t *hash,
                                 ErlNifBinary* key,
                                 khiter_t* itr_ptr,
                                 bitcask_keydir_entry** entry_ptr)
{
    //suspicious of the optimization here, lowered to 256
    khiter_t itr;
    if (key->size < (256 - sizeof(bitcask_keydir_entry)))
    {
        char buf[256];
        bitcask_keydir_entry* e = (bitcask_keydir_entry*)buf;
        e->key_sz = key->size;
        memcpy(e->key, key->data, key->size);
        itr = kh_get(entries, hash, e);
    }
    else
    {
        bitcask_keydir_entry* e = enif_alloc_compat(env, sizeof(bitcask_keydir_entry) +
                                                    key->size);
        e->key_sz = key->size;
        memcpy(e->key, key->data, key->size);
        itr = kh_get(entries, hash, e);
        enif_free_compat(env, e);
    }
  
    //fprintf(stderr, "geh sez: itr %d, end %d\n", itr, kh_end(hash));

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

static int find_keydir_entry(ErlNifEnv* env, bitcask_keydir* keydir, ErlNifBinary* key,
                             entries_hash_t** hash_ptr, khiter_t* itr_ptr,
                             bitcask_keydir_entry** entry_ptr)
{
    if (get_entries_hash(env, keydir->entries, key, itr_ptr, entry_ptr))
    {
        if (hash_ptr != NULL) 
        {
            *hash_ptr = keydir->entries;
        }
        return 1;
    }
    
    return 0;
}

// Allocate, populate and add entry to the keydir hash based on the key and entry structure
static bitcask_keydir_entry* add_entry(ErlNifEnv* env, bitcask_keydir* keydir,
                                       entries_hash_t* hash,
                                       ErlNifBinary* key, bitcask_keydir_entry* entry)
{
    if (keydir->keyfolders == 0) {

        bitcask_keydir_entry* new_entry = enif_alloc_compat(env,
                                                            sizeof(bitcask_keydir_entry) +
                                                            key->size);
        new_entry->file_id = entry->file_id;
        new_entry->total_sz = entry->total_sz;
        new_entry->offset = entry->offset;
        new_entry->tstamp = entry->tstamp;
        new_entry->key_sz = key->size;
        memcpy(new_entry->key, key->data, key->size);
        kh_put_set(entries, hash, new_entry);

        return new_entry;
    }
    else
    {
        bitcask_keydir_parent * p = NULL;
        bitcask_keydir_entry * e = NULL; 
        //this is not a full entry
        entry->key_sz = key->size;
        memcpy(entry->key, key->data, key->size);
        p = make_entry_list(env, entry);
        //fprintf(stderr, "adding new entry list pointer %p from entry %p\n", p, entry);
        e = MAKE_ENTRY_LIST_POINTER(p);
        kh_put_set(entries, hash, e);

        return e;
    }
}

// Update the current entry with newer information
static void update_entry(ErlNifEnv* env, bitcask_keydir* keydir,
                         bitcask_keydir_entry* cur_entry,
                         bitcask_keydir_entry* upd_entry)
{
    if (keydir->keyfolders == 0) { 
        if (IS_ENTRY_LIST(cur_entry)) 
        {
            bitcask_keydir_entry * e = NULL;
            e = enif_alloc_compat(env,
                                  sizeof(bitcask_keydir_entry) + 
                                  upd_entry->key_sz);
            e->file_id = upd_entry->file_id;
            e->total_sz = upd_entry->total_sz;
            e->offset = upd_entry->offset;
            e->tstamp = upd_entry->tstamp;
            e->key_sz = upd_entry->key_sz;
            memcpy(e->key, upd_entry->key, e->key_sz);

            khiter_t itr = kh_get(entries, keydir->entries, cur_entry);
            kh_del(entries, keydir->entries, itr);
            kh_put_set(entries, keydir->entries, e);

            free_entry_list(env, GET_ENTRY_LIST_POINTER(cur_entry));
        }
        else
        {
            cur_entry->file_id = upd_entry->file_id;
            cur_entry->total_sz = upd_entry->total_sz;
            cur_entry->offset = upd_entry->offset;
            cur_entry->tstamp = upd_entry->tstamp;
        }
    }
    else
    {
        if (IS_ENTRY_LIST(cur_entry)) {
            bitcask_keydir_parent* p = NULL;
            p = GET_ENTRY_LIST_POINTER(cur_entry);

            bitcask_keydir_sib* s = p->sibs;

            // if newer than the newest fold, update it
            if (s->tstamp >= keydir->newest_folder)
            { 
                s->file_id = upd_entry->file_id;
                s->total_sz = upd_entry->total_sz;
                s->offset = upd_entry->offset;
                s->tstamp = upd_entry->tstamp;
            }
            else
            // else make a new sib and prepend to the sib list
            {
                bitcask_keydir_sib* new_sib = NULL;
                new_sib = enif_alloc_compat(env, sizeof(bitcask_keydir_sib));
                new_sib->next = p->sibs;
                p->sibs = new_sib;

                new_sib->file_id = upd_entry->file_id;
                new_sib->total_sz = upd_entry->total_sz;
                new_sib->offset = upd_entry->offset;
                new_sib->tstamp = upd_entry->tstamp;
            }
        }
        else
        {
            bitcask_keydir_parent* p = NULL;
            p = make_entry_list(env, cur_entry);
            bitcask_keydir_entry* e = NULL;
            //fprintf(stderr, "updating entry %p into entry list %p\n", cur_entry, p);
            //fprintf(stderr, "%s %s %d %d", cur_entry->key, p->key, cur_entry->key_sz,
            //p->key_sz);
            e = (bitcask_keydir_entry*)MAKE_ENTRY_LIST_POINTER(p); 

            khiter_t itr = kh_get(entries, keydir->entries, cur_entry);
            kh_del(entries, keydir->entries, itr);
            kh_put_set(entries, keydir->entries, e);

            enif_free_compat(env, cur_entry);

            bitcask_keydir_sib* s = NULL;
            s = enif_alloc_compat(env, sizeof(bitcask_keydir_sib));
            s->next = p->sibs;
            p->sibs = s;
            
            s->file_id = upd_entry->file_id;
            s->total_sz = upd_entry->total_sz;
            s->offset = upd_entry->offset;
            s->tstamp = upd_entry->tstamp;

        }            
    }
}

static void remove_entry(ErlNifEnv* env, bitcask_keydir* keydir, khiter_t itr,
                         uint32_t ts, bitcask_keydir_entry* entry, 
                         bitcask_keydir_parent* real_entry)
{
    if (keydir->keyfolders == 0) 
    {
        kh_del(entries, keydir->entries, itr);
        if(real_entry == NULL) { 
            enif_free_compat(env, entry);
        } 
        else
        {
            //fprintf(stderr, "removing entry %p", real_entry);
            free_entry_list(env, real_entry);
        }
    }
    else
    { 
        if (real_entry == NULL) 
        {
            //need to convert to a entry_list
            bitcask_keydir_parent* p = make_entry_list(env, entry);
            enif_free_compat(env, entry);

            bitcask_keydir_sib* s = make_sib_tombstone(env, ts);
            s->next = p->sibs;
            p->sibs = s;
            //do we need to munge the pointer here or elsewhere?
            entry = (bitcask_keydir_entry*)p;
        }
        else 
        {
            bitcask_keydir_sib* s = make_sib_tombstone(env, ts);
            s->next = real_entry->sibs;
            real_entry->sibs = s;
        }
    }
}

static bitcask_keydir_parent* make_entry_list(ErlNifEnv * env, 
                                              bitcask_keydir_entry* entry) 
{
    bitcask_keydir_parent* p = NULL;
    p = enif_alloc_compat(env, sizeof(bitcask_keydir_parent)+entry->key_sz);
    bitcask_keydir_sib* s = NULL;
    s = enif_alloc_compat(env, sizeof(bitcask_keydir_sib));

    p->sibs = s;
    s->next = NULL;

    memcpy(p->key, entry->key, entry->key_sz);
    p->key_sz = entry->key_sz;
    //fprintf(stderr, "making entry list w/ key_sz: %d %d", entry->key_sz, p->key_sz);

    s->file_id = entry->file_id;
    s->total_sz = entry->total_sz;
    s->offset = entry->offset;
    s->tstamp =  entry->tstamp;
    s->newest_put = entry->newest_put;
    
    return p;              
}

static void free_entry_list(ErlNifEnv* env, bitcask_keydir_parent* p) 
{
    
    //fprintf(stderr, "freeing entry_list with parent %p\n", p);
    bitcask_keydir_sib
        *temp = NULL, 
        *s = p->sibs;
    while (s != NULL) {
        //fprintf(stderr, "freeing %p, next %p \n", s, s->next);
        temp = s;
        s = s->next;

        enif_free_compat(env, temp);
    }
    
    enif_free_compat(env, p);
    //fprintf(stderr, "finishing up freeing\n");
}

static bitcask_keydir_entry* make_synthetic_entry(ErlNifEnv * env,
                                                  bitcask_keydir_parent *p,
                                                  uint32_t tstamp)
{

    //fprintf(stderr, "making synth entry @ %u key: %s\n", tstamp, p->key);
    //find the right sib here.
    bitcask_keydir_sib* s = p->sibs;
    while (s != NULL) 
    { 
        //fprintf(stderr, "sib @ %u\n", s->tstamp);
        if (s->tstamp <= tstamp) 
            break;
        s = s->next;
    } 
    if (s == NULL) 
    {
        return NULL;
    }
    
    bitcask_keydir_entry* ret = NULL;
    ret = enif_alloc_compat(env, sizeof(bitcask_keydir_entry)+p->key_sz);
    
    memcpy(ret->key, p->key, p->key_sz);
    ret->key_sz = p->key_sz;
 
    ret->file_id = s->file_id;
    ret->total_sz = s->total_sz;
    ret->offset = s->offset;
    ret->tstamp =  s->tstamp;
    ret->newest_put = s->newest_put;

    return ret;
}

static bitcask_keydir_sib* make_sib_tombstone(ErlNifEnv * env, uint32_t ts) {
    bitcask_keydir_sib* s = enif_alloc_compat(env, sizeof(bitcask_keydir_sib));
    if (s == NULL) {
        return NULL;
    }
    s->file_id = 0xffffffff;
    s->total_sz = 0xffffffff;
    s->offset = 0xffffffffffffffff;
    s->tstamp = ts;
    return s;
}

// This function isn't thread-safe, but assumes that the functions that 
// call it are.
static int get_entry(ErlNifEnv* env, bitcask_keydir_entry* src, 
                     bitcask_keydir_entry** dest, uint32_t ts)
{
    //fprintf(stderr, "getting entry: %p\n", src);
    if (IS_ENTRY_LIST(src)) 
    {
        bitcask_keydir_parent* parent = GET_ENTRY_LIST_POINTER(src);
        bitcask_keydir_sib* ptr = parent->sibs;
        
        while (ptr != NULL) 
        { 
            if (ptr->tstamp <= ts) 
            {
                break;
            }
            ptr = (bitcask_keydir_sib *)ptr->next;
        }
        if (ptr != NULL) {
            bitcask_keydir_entry *dest0 = NULL; 
            dest0 = enif_alloc_compat(env, 
                                     sizeof(bitcask_keydir_entry) + 
                                     parent->key_sz);
            dest0->key_sz = parent->key_sz;
            memcpy(dest0->key, parent->key, parent->key_sz);
            dest0->file_id = ptr->file_id;
            dest0->total_sz = ptr->total_sz;
            dest0->offset = ptr->offset;
            dest0->tstamp = ptr->tstamp;
            dest0->newest_put = ptr->newest_put;
            *dest = dest0;
            return 1;
        } 
        else
        {
            return 0;
        }
    }
    else 
    {
        int sz = src->key_sz; 
        bitcask_keydir_entry* neu = enif_alloc_compat(env, 
                                                      sizeof(bitcask_keydir_entry) + 
                                                      sz);
        memcpy(neu, src, sizeof(bitcask_keydir_entry) + sz);
        *dest = neu;
        return 1;
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_put_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    bitcask_keydir_entry entry;
    ErlNifBinary key;
    uint32_t old_file_id;
    uint64_t old_offset;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint(env, argv[2], (unsigned int*)&(entry.file_id)) &&
        enif_get_uint(env, argv[3], &(entry.total_sz)) &&
        enif_get_uint64_bin(env, argv[4], &(entry.offset)) &&
        enif_get_uint(env, argv[5], &(entry.tstamp)) &&
        enif_get_uint(env, argv[6], &(entry.newest_put)) &&
        enif_get_uint(env, argv[7], (unsigned int*)&(old_file_id)) &&
        enif_get_uint64_bin(env, argv[8], &(old_offset)))
    {
        khiter_t itr;
        entries_hash_t* hash;
        bitcask_keydir_entry* old_entry0;
        bitcask_keydir_entry* old_entry;
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);
        //fprintf(stderr, "putting %2s\n", key.data); 

        DEBUG("+++ Put file_id=%d offset=%d total_sz=%d\r\n",
              (int) entry.file_id, (int) entry.offset,
              (int)entry.total_sz);

        // Check for put on a new key or updating 
        int old_entry_synthetic = 0;
        int found = find_keydir_entry(env, keydir, &key, &hash, &itr, &old_entry0);
        if (found && IS_ENTRY_LIST(old_entry0))
        {
            old_entry = make_synthetic_entry(env, GET_ENTRY_LIST_POINTER(old_entry0),
                                             0xffffffff);
            old_entry_synthetic = 1;
        } 
        else if (found) 
        {
            old_entry = old_entry0;
        }

        if (!found && old_file_id != 0)
        {
            UNLOCK(keydir);
            return ATOM_ALREADY_EXISTS;
        }

        if (!found)
        {
            keydir->key_count++;
            keydir->key_bytes += key.size;

            // Increment live and total stats.
            update_fstats(env, keydir, entry.file_id, entry.tstamp,
                          1, 1, entry.total_sz, entry.total_sz);

            if (entry.file_id > keydir->biggest_file_id)
            {
                keydir->biggest_file_id = entry.file_id;
            }

            add_entry(env, keydir, keydir->entries, &key, &entry);

            UNLOCK(keydir);
            return ATOM_OK;
        }

        // If old_file_id is > 0, then test-and-set fails,
        // then return already_exists.
        if (old_file_id != 0 &&
            !(old_file_id == old_entry->file_id &&
              old_offset == old_entry->offset))
        {
            UNLOCK(keydir);
            if (old_entry_synthetic)
            {
                enif_free_compat(env, old_entry);
            }
            return ATOM_ALREADY_EXISTS;
        }

        // Now that we've marshalled everything, see if the tstamp for this key is >=
        // to what's already in the hash. Otherwise, we don't bother with the update.
        if ((entry.newest_put &&
             (entry.file_id >= keydir->biggest_file_id)) ||
            (! entry.newest_put &&
             (old_entry->tstamp < entry.tstamp)) ||
            (! entry.newest_put &&
             ((old_entry->file_id < entry.file_id) ||
              (((old_entry->file_id == entry.file_id) &&
                (old_entry->offset < entry.offset))))))
        {
            // Remove the stats for the old entry and add the new
            if (old_entry->file_id != entry.file_id) // different files
            {
                update_fstats(env, keydir, old_entry->file_id, 0,
                              -1, 0,
                              -old_entry->total_sz, 0);
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              1, 1,
                              entry.total_sz, entry.total_sz);
            }
            else // file_id is same, change live/total in one entry
            {
                update_fstats(env, keydir, entry.file_id, 0,
                              0, 1,
                              entry.total_sz - old_entry->total_sz,
                              entry.total_sz);
            }

            update_entry(env, keydir, old_entry, &entry);

            if (entry.file_id > keydir->biggest_file_id)
            {
                keydir->biggest_file_id = entry.file_id;
            }

            UNLOCK(keydir);
            if (old_entry_synthetic)
            {
                enif_free_compat(env, old_entry);
            }
            return ATOM_OK;
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
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              0, 1, 0, entry.total_sz);
            }
            UNLOCK(keydir);
            if (old_entry_synthetic)
            {
                enif_free_compat(env, old_entry);
            }
            return ATOM_ALREADY_EXISTS;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}


ERL_NIF_TERM bitcask_nifs_keydir_get_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    ErlNifBinary key;
    uint32_t time;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint(env, argv[2], &time))
    {
        //fprintf(stderr, "getting @ %d\n", time);
        bitcask_keydir_entry* entry = NULL;
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);

        DEBUG("+++ Get issued\r\n");
        if (find_keydir_entry(env, keydir, &key, NULL, NULL, &entry))
        {
            //fprintf(stderr, "found %p\n", entry);
            ERL_NIF_TERM result;
            if (!IS_ENTRY_LIST(entry))
            {
                //if it isn't an entry list, ignore the requested timestamp,
                result = enif_make_tuple6(env,
                                          ATOM_BITCASK_ENTRY,
                                          argv[1], /* Key */
                                          enif_make_uint(env, entry->file_id),
                                          enif_make_uint(env, entry->total_sz),
                                          enif_make_uint64_bin(env, 
                                                               entry->offset),
                                          enif_make_uint(env, entry->tstamp));
            }
            else
            {
                bitcask_keydir_entry* e = NULL;
                e = make_synthetic_entry(env, GET_ENTRY_LIST_POINTER(entry), 
                                         time);
                if (e != NULL) {
                    result = enif_make_tuple6(env,
                                              ATOM_BITCASK_ENTRY,
                                              argv[1], /* Key */
                                              enif_make_uint(env, e->file_id),
                                              enif_make_uint(env, e->total_sz),
                                              enif_make_uint64_bin(env, 
                                                                   e->offset),
                                              enif_make_uint(env, e->tstamp));
 
                    DEBUG(" ... returned value\r\n");
                    enif_free_compat(env, e);
                }
                else
                {
                    result = ATOM_NOT_FOUND;

                }
            }
            UNLOCK(keydir);
            return result;
        }
        else
        {
            DEBUG(" ... not_found\r\n");
            UNLOCK(keydir);
            return ATOM_NOT_FOUND;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}


ERL_NIF_TERM bitcask_nifs_keydir_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    ErlNifBinary key;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key))
    {
        khiter_t itr;
        entries_hash_t* hash;
        bitcask_keydir_entry* entry;
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);

        DEBUG("+++ Remove\r\n");
        //fprintf(stderr, "removing %2s\n", key.data); 

        if (find_keydir_entry(env, keydir, &key, &hash, &itr, &entry))
        {
            bitcask_keydir_parent* real_entry = NULL;
            if (IS_ENTRY_LIST(entry))
            {
                real_entry = GET_ENTRY_LIST_POINTER(entry);
                entry = make_synthetic_entry(env, real_entry, 0xffffffff);
            }
            //fprintf(stderr, "found %2s, entry list: %d\n", key.data, (int)IS_ENTRY_LIST(entry)); 
                    
            // If this call has 5 arguments, this is a conditional removal. We
            // only want to actually remove the entry if the tstamp, fileid and
            // offset matches the one provided. A sort of poor-man's CAS.
            if (argc == 5)
            {
                uint32_t tstamp;
                uint32_t file_id;
                uint64_t offset;
                if (enif_get_uint(env, argv[2], (unsigned int*)&tstamp) &&
                    enif_get_uint(env, argv[3], (unsigned int*)&file_id) &&
                    enif_get_uint64_bin(env, argv[4], (uint64_t*)&offset))
                {
                    if (entry->tstamp != tstamp || entry->file_id != file_id ||
                        entry->offset != offset)
                    {
                        // Either tstamp or file_id didn't match precisely. Ignore
                        // this attempt to delete the record.
                        if (real_entry != NULL) 
                        { 
                            enif_free_compat(env, entry);
                        }
                        UNLOCK(keydir);
                        return ATOM_OK;
                    }
                }
                else
                {
                    if (real_entry != NULL) 
                    { 
                        enif_free_compat(env, entry);
                    }
                    UNLOCK(keydir);
                    return enif_make_badarg(env);
                }
            }

            // Remove the key from the keydir stats
            keydir->key_count--;
            keydir->key_bytes -= entry->key_sz;

            // Remove from file stats
            update_fstats(env, keydir, entry->file_id, entry->tstamp,
                          -1, 0, -entry->total_sz, 0);

            // may need the current timestamp.
            uint32_t ts = time(NULL);

            // since this has access to the locked keydir, it 
            // takes care of chaining/tombstoning.
            remove_entry(env, keydir, itr, ts, entry, real_entry);
            
            if (real_entry != NULL) 
            { 
                enif_free_compat(env, entry);
            }
            UNLOCK(keydir);
            return ATOM_OK;
        }
        else // entry not found, should not get here in normal operation nothing to update
        {
            UNLOCK(keydir);
            return ATOM_OK;;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_copy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);

        bitcask_keydir_handle* new_handle = enif_alloc_resource_compat(env,
                                                                bitcask_keydir_RESOURCE,
                                                                sizeof(bitcask_keydir_handle));
        memset(handle, '\0', sizeof(bitcask_keydir_handle));

        // Now allocate the actual keydir instance. Because it's unnamed/shared, we'll
        // leave the name and lock portions null'd out
        bitcask_keydir* new_keydir = enif_alloc_compat(env, sizeof(bitcask_keydir));
        new_handle->keydir = new_keydir;
        memset(new_keydir, '\0', sizeof(bitcask_keydir));
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
                bitcask_keydir_entry* new = enif_alloc_compat(env, new_sz);
                memcpy(new, curr, new_sz);
                kh_put_set(entries, new_keydir->entries, new);
            }
        }

        // Deep copy fstats info
        for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
        {
            if (kh_exist(keydir->fstats, itr))
            {
                bitcask_fstats_entry* curr_f = kh_val(keydir->fstats, itr);
                bitcask_fstats_entry* new_f = enif_alloc_compat(env,
                                                                sizeof(bitcask_fstats_entry));
                memcpy(new_f, curr_f, sizeof(bitcask_fstats_entry));
                kh_put2(fstats, new_keydir->fstats, new_f->file_id, new_f);
            }
        }

        UNLOCK(keydir);

        ERL_NIF_TERM result = enif_make_resource(env, new_handle);
        enif_release_resource_compat(env, new_handle);
        return enif_make_tuple2(env, ATOM_OK, result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    //fprintf(stderr, "creating iterator\n");

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        uint64_t ts;
        int maxage;
        int maxputs;

        LOCK(handle->keydir);
        DEBUG("+++ itr\r\n");
        bitcask_keydir* keydir = handle->keydir;

        if (!(enif_get_uint64_bin(env, argv[1], (uint64_t*)&ts) &&
              enif_get_int(env, argv[2], (int*)&maxage) &&
              enif_get_int(env, argv[3], (int*)&maxputs)))
        {
            UNLOCK(handle->keydir);
            return enif_make_badarg(env);
        }

        // if we're not in fold-mode yet, put us into fold-mode and 
        // update the newest_folder timestamp.

        keydir->newest_folder = ts;
        keydir->keyfolders++;

        handle->iterating = 1;
        handle->iterator = kh_begin(keydir->entries);
        handle->ts = ts;
        
        UNLOCK(handle->keydir);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    
    //fprintf(stderr, "entering itr_next\n");
    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);
        if (handle->iterating != 1)
        {
            // Iteration not started!
            UNLOCK(keydir);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_NOT_STARTED);
        }

        while (handle->iterator != kh_end(keydir->entries))
        {
            if (kh_exist(keydir->entries, handle->iterator))
            {
                
                bitcask_keydir_entry* entry = NULL;
                if(!get_entry(env, kh_key(keydir->entries, handle->iterator), 
                              &entry, handle->ts)) 
                {
                    UNLOCK(keydir); 
                    return ATOM_ALLOCATION_ERROR;
                }

                (handle->iterator)++;
                UNLOCK(keydir);

                ErlNifBinary key;

                // Alloc the binary and make sure it succeeded
                if (!enif_alloc_binary_compat(env, entry->key_sz, &key))
                {
                    return ATOM_ALLOCATION_ERROR;
                }

                // Copy the data from our key to the new allocated binary
                memcpy(key.data, entry->key, entry->key_sz);
                ERL_NIF_TERM curr = enif_make_tuple6(env,
                                                     ATOM_BITCASK_ENTRY,
                                                     enif_make_binary(env, &key),
                                                     enif_make_uint(env, entry->file_id),
                                                     enif_make_uint(env, entry->total_sz),
                                                     enif_make_uint64_bin(env, entry->offset),
                                                     enif_make_uint(env, entry->tstamp));
                // get_entry always allocates the entry, must free now that we've copied.
                enif_free_compat(env, entry);

                // Update the iterator to the next entry
                return curr;
            }
            else
            {
                // No item in this slot; increment the iterator and keep looping
                (handle->iterator)++;
            }
        }

        // The iterator is at the end of the table
        UNLOCK(keydir);
        return ATOM_NOT_FOUND;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_itr_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    //fprintf(stderr, "ending iterator\n");
        
    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        LOCK(handle->keydir);
        if (handle->iterating != 1)
        {
            // Iteration not started!
            UNLOCK(handle->keydir);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_NOT_STARTED);
        }

        handle->iterating = 0;
        handle->keydir->keyfolders--; 
        if (handle->keydir->keyfolders == 0)
        {
            handle->keydir->iter_generation++;
        }
        UNLOCK(handle->keydir);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}


ERL_NIF_TERM bitcask_nifs_keydir_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        bitcask_keydir* keydir = handle->keydir;

        if (keydir == NULL)
        {
            return enif_make_badarg(env);
        }
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
                             keydir->keyfolders == 0 ? ATOM_FALSE : ATOM_TRUE);
        ERL_NIF_TERM result = enif_make_tuple4(env,
                                               enif_make_ulong(env, keydir->key_count),
                                               enif_make_ulong(env, keydir->key_bytes),
                                               fstats_list,
                                               iter_info);
        UNLOCK(keydir);
        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        bitcask_nifs_keydir_resource_cleanup(env, handle);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_lock_acquire(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char filename[4096];
    int is_write_lock = 0;
    if (enif_get_string(env, argv[0], filename, sizeof(filename), ERL_NIF_LATIN1) > 0 &&
        enif_get_int(env, argv[1], &is_write_lock))
    {
        // Setup the flags for the lock file
        int flags = O_RDONLY;
        if (is_write_lock)
        {
            // Use O_SYNC (in addition to other flags) to ensure that when we write
            // data to the lock file it is immediately (or nearly) available to any
            // other reading processes
            flags = O_CREAT | O_EXCL | O_RDWR | O_SYNC;
        }

        // Try to open the lock file -- allocate a resource if all goes well.
        int fd = open(filename, flags, 0600);
        if (fd > -1)
        {
            // Successfully opened the file -- setup a resource to track the FD.
            unsigned int filename_sz = strlen(filename) + 1;
            bitcask_lock_handle* handle = enif_alloc_resource_compat(env, bitcask_lock_RESOURCE,
                                                              sizeof(bitcask_lock_handle) +
                                                              filename_sz);
            handle->fd = fd;
            handle->is_write_lock = is_write_lock;
            strncpy(handle->filename, filename, filename_sz);
            ERL_NIF_TERM result = enif_make_resource(env, handle);
            enif_release_resource_compat(env, handle);

            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_lock_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_lock_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_lock_RESOURCE, (void**)&handle))
    {
        lock_release(handle);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_lock_readdata(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_lock_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_lock_RESOURCE, (void**)&handle))
    {
        // Stat the filehandle so we can read the entire contents into memory
        struct stat sinfo;
        if (fstat(handle->fd, &sinfo) != 0)
        {
            return errno_error_tuple(env, ATOM_FSTAT_ERROR, errno);
        }

        // Allocate a binary to hold the contents of the file
        ErlNifBinary data;
        if (!enif_alloc_binary_compat(env, sinfo.st_size, &data))
        {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
        }

        // Read the whole file into our binary
        if (pread(handle->fd, data.data, data.size, 0) == -1)
        {
            return errno_error_tuple(env, ATOM_PREAD_ERROR, errno);
        }

        return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &data));
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_lock_writedata(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_lock_handle* handle;
    ErlNifBinary data;

    if (enif_get_resource(env, argv[0], bitcask_lock_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &data))
    {
        if (handle->is_write_lock)
        {
            // Truncate the file first, to ensure that the lock file only contains what
            // we're about to write
            if (ftruncate(handle->fd, 0) == -1)
            {
                return errno_error_tuple(env, ATOM_FTRUNCATE_ERROR, errno);
            }

            // Write the new blob of data to the lock file. Note that we use O_SYNC to
            // ensure that the data is available ASAP to reading processes.
            if (pwrite(handle->fd, data.data, data.size, 0) == -1)
            {
                return errno_error_tuple(env, ATOM_PWRITE_ERROR, errno);
            }

            return ATOM_OK;
        }
        else
        {
            // Tried to write data to a read lock
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_LOCK_NOT_WRITABLE);
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

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


ERL_NIF_TERM bitcask_nifs_file_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char filename[4096];
    if (enif_get_string(env, argv[0], filename, sizeof(filename), ERL_NIF_LATIN1) &&
        enif_is_list(env, argv[1]))
    {
        int flags = get_file_open_flags(env, argv[1]);
        int fd = open(filename, flags, S_IREAD | S_IWRITE);
        if (fd > -1)
        {
            // Setup a resource for our handle
            bitcask_file_handle* handle = enif_alloc_resource_compat(env,
                                                                     bitcask_file_RESOURCE,
                                                                     sizeof(bitcask_file_handle));
            memset(handle, '\0', sizeof(bitcask_file_handle));
            handle->fd = fd;

            ERL_NIF_TERM result = enif_make_resource(env, handle);
            enif_release_resource_compat(env, handle);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle))
    {
        if (handle->fd > 0)
        {
            /* TODO: Check for EIO */
            close(handle->fd);
            handle->fd = -1;
        }
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle))
    {
        int rc = fsync(handle->fd);
        if (rc != -1)
        {
            return ATOM_OK;
        }
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_pread(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    unsigned long offset_ul;
    unsigned long count_ul;
    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &offset_ul) && /* Offset */
        enif_get_ulong(env, argv[2], &count_ul))    /* Count */
    {
        ErlNifBinary bin;
        off_t offset = offset_ul;
        size_t count = count_ul;
        if (!enif_alloc_binary(count, &bin))
        {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
        }

        ssize_t bytes_read = pread(handle->fd, bin.data, count, offset);
        if (bytes_read == count)
        {
            /* Good read; return {ok, Bin} */
            return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
        }
        else if (bytes_read > 0)
        {
            /* Partial read; need to resize our binary (bleh) and return {ok, Bin} */
            if (enif_realloc_binary(&bin, bytes_read))
            {
                return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
            }
            else
            {
                /* Realloc failed; cleanup and bail */
                enif_release_binary(&bin);
                return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
            }
        }
        else if (bytes_read == 0)
        {
            /* EOF */
            enif_release_binary(&bin);
            return ATOM_EOF;
        }
        else
        {
            /* Read failed altogether */
            enif_release_binary(&bin);
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_pwrite(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    unsigned long offset_ul;
    ErlNifBinary bin;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &offset_ul) && /* Offset */
        enif_inspect_iolist_as_binary(env, argv[2], &bin)) /* Bytes to write */
    {
        unsigned char* buf = bin.data;
        ssize_t bytes_written = 0;
        ssize_t count = bin.size;
        off_t offset = offset_ul;

        while (count > 0)
        {
            bytes_written = pwrite(handle->fd, buf, count, offset);
            if (bytes_written > 0)
            {
                count -= bytes_written;
                offset += bytes_written;
                buf += bytes_written;
            }
            else
            {
                /* Write failed altogether */
                return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
            }
        }

        /* Write done */
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    size_t count;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &count))    /* Count */
    {
        ErlNifBinary bin;
        if (!enif_alloc_binary(count, &bin))
        {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
        }

        ssize_t bytes_read = read(handle->fd, bin.data, count);
        if (bytes_read == count)
        {
            /* Good read; return {ok, Bin} */
            return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
        }
        else if (bytes_read > 0)
        {
            /* Partial read; need to resize our binary (bleh) and return {ok, Bin} */
            if (enif_realloc_binary(&bin, bytes_read))
            {
                return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
            }
            else
            {
                /* Realloc failed; cleanup and bail */
                enif_release_binary(&bin);
                return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
            }
        }
        else if (bytes_read == 0)
        {
            /* EOF */
            enif_release_binary(&bin);
            return ATOM_EOF;
        }
        else
        {
            /* Read failed altogether */
            enif_release_binary(&bin);
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    ErlNifBinary bin;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle) &&
        enif_inspect_iolist_as_binary(env, argv[1], &bin)) /* Bytes to write */
    {
        unsigned char* buf = bin.data;
        ssize_t bytes_written = 0;
        ssize_t count = bin.size;
        while (count > 0)
        {
            bytes_written = write(handle->fd, buf, count);
            if (bytes_written > 0)
            {
                count -= bytes_written;
                buf += bytes_written;
            }
            else
            {
                /* Write failed altogether */
                return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
            }
        }

        /* Write done */
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    unsigned long offset_ul;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &offset_ul))
    {

        off_t offset = offset_ul;
        off_t new_offset = lseek(handle->fd, offset, SEEK_SET);
        if (new_offset != -1)
        {
            return enif_make_tuple2(env, ATOM_OK, enif_make_ulong(env, new_offset));
        }
        else
        {
            /* Write failed altogether */
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_file_seekbof(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle))
    {
        if (lseek(handle->fd, 0, SEEK_SET) != -1)
        {
            return ATOM_OK;
        }
        else
        {
            /* Write failed altogether */
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}


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
            if (IS_ENTRY_LIST(current_entry))
            {
                bitcask_keydir_parent *p = GET_ENTRY_LIST_POINTER(current_entry);
                free_entry_list(env, p);
            }
            else
            {
                enif_free_compat(env, current_entry);
            }
        }
    }

    kh_destroy(entries, keydir->entries);

    bitcask_fstats_entry* curr_f;

    for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
    {
        if (kh_exist(keydir->fstats, itr))
        {
            curr_f = kh_val(keydir->fstats, itr);
            enif_free_compat(env, curr_f);
        }
    }

    kh_destroy(fstats, keydir->fstats);
}


static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg)
{
    bitcask_keydir_handle* handle = (bitcask_keydir_handle*)arg;
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
        bitcask_priv_data* priv = (bitcask_priv_data*)enif_priv_data(env);
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
    bitcask_keydir_RESOURCE = enif_open_resource_type_compat(env, "bitcask_keydir_resource",
                                                      &bitcask_nifs_keydir_resource_cleanup,
                                                      ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                      0);

    bitcask_lock_RESOURCE = enif_open_resource_type_compat(env, "bitcask_lock_resource",
                                                    &bitcask_nifs_lock_resource_cleanup,
                                                    ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                    0);

    bitcask_file_RESOURCE = enif_open_resource_type_compat(env, "bitcask_file_resource",
                                                    &bitcask_nifs_file_resource_cleanup,
                                                    ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                    0);

    // Initialize shared keydir hashtable
    bitcask_priv_data* priv = enif_alloc_compat(env, sizeof(bitcask_priv_data));
    priv->global_keydirs = kh_init(global_keydirs);
    priv->global_keydirs_lock = enif_mutex_create("bitcask_global_handles_lock");
    *priv_data = priv;

    // Initialize atoms that we use throughout the NIF.
    ATOM_ALLOCATION_ERROR = enif_make_atom(env, "allocation_error");
    ATOM_ALREADY_EXISTS = enif_make_atom(env, "already_exists");
    ATOM_BITCASK_ENTRY = enif_make_atom(env, "bitcask_entry");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_FSTAT_ERROR = enif_make_atom(env, "fstat_error");
    ATOM_FTRUNCATE_ERROR = enif_make_atom(env, "ftruncate_error");
    ATOM_GETFL_ERROR = enif_make_atom(env, "getfl_error");
    ATOM_ILT_CREATE_ERROR = enif_make_atom(env, "ilt_create_error");
    ATOM_ITERATION_NOT_STARTED = enif_make_atom(env, "iteration_not_started");
    ATOM_LOCK_NOT_WRITABLE = enif_make_atom(env, "lock_not_writable");
    ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
    ATOM_NOT_READY = enif_make_atom(env, "not_ready");
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_OUT_OF_DATE = enif_make_atom(env, "out_of_date");
    ATOM_PREAD_ERROR = enif_make_atom(env, "pread_error");
    ATOM_PWRITE_ERROR = enif_make_atom(env, "pwrite_error");
    ATOM_READY = enif_make_atom(env, "ready");
    ATOM_SETFL_ERROR = enif_make_atom(env, "setfl_error");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_EOF = enif_make_atom(env, "eof");
    ATOM_CREATE = enif_make_atom(env, "create");
    ATOM_READONLY = enif_make_atom(env, "readonly");
    ATOM_O_SYNC = enif_make_atom(env, "o_sync");

#ifdef PULSE
	pulse_c_send_on_load(env);
#endif

    return 0;
}

ERL_NIF_INIT(bitcask_nifs, nif_funcs, &on_load, NULL, NULL, NULL);


