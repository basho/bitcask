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
#include <time.h>
#include <assert.h>

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

typedef struct
{
    uint32_t file_id;
    uint32_t total_sz;
    uint64_t offset;
    uint32_t tstamp;
    uint16_t key_sz;
    char *   key;
    bitcask_keydir_entry * entry;
} bitcask_keydir_entry_proxy;

struct bitcask_keydir_entry_sib
{
    uint32_t file_id;
    uint32_t total_sz;
    uint64_t offset;
    uint32_t tstamp;
    struct bitcask_keydir_entry_sib * next;
};
typedef struct bitcask_keydir_entry_sib bitcask_keydir_entry_sib;

typedef struct
{
    bitcask_keydir_entry_sib * sibs;
    uint16_t key_sz;
    char     key[0];
} bitcask_keydir_entry_head;

#define IS_ENTRY_LIST(p) ((uint64_t)p&1)
#define GET_ENTRY_LIST_POINTER(p) ((bitcask_keydir_entry_head*)((uint64_t)p&~1))
#define MAKE_ENTRY_LIST_POINTER(p) ((bitcask_keydir_entry*)((uint64_t)p|1))

KHASH_MAP_INIT_INT(fstats, bitcask_fstats_entry*);

typedef khash_t(entries) entries_hash_t;
typedef khash_t(fstats) fstats_hash_t;

typedef struct
{
    entries_hash_t* entries;
    entries_hash_t* pending;  // pending keydir entries during keydir folding
    fstats_hash_t*  fstats;
    uint32_t      key_count;
    uint32_t      key_bytes;
    uint32_t      biggest_file_id;
    unsigned int  refcount;
    unsigned int  keyfolders;
    uint32_t      newest_folder;
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
    uint32_t        timestamp;
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

#define TRUE 1
#define FALSE 0

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
static ERL_NIF_TERM ATOM_ITERATION_IN_PROCESS;
static ERL_NIF_TERM ATOM_ITERATION_NOT_PERMITTED;
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

static void merge_pending_entries(ErlNifEnv* env, bitcask_keydir* keydir);
static void lock_release(bitcask_lock_handle* handle);

static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg);
static void bitcask_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg);

/*static void print_entry_list(bitcask_keydir_entry *e);*/

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
    {"keydir_remove", 3, bitcask_nifs_keydir_remove},
    {"keydir_remove_int", 6, bitcask_nifs_keydir_remove},
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
    bitcask_keydir* keydir = malloc(sizeof(bitcask_keydir));
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
            keydir = malloc(sizeof(bitcask_keydir) + name_sz + 1);
            memset(keydir, '\0', sizeof(bitcask_keydir) + name_sz + 1);
            strncpy(keydir->name, name, name_sz + 1);

            // Initialize hash tables
            keydir->entries  = kh_init(entries);
            keydir->fstats   = kh_init(fstats);

            // Be sure to initialize the mutex and set our refcount
            keydir->mutex = enif_mutex_create(name);
            keydir->refcount = 1;

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

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
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
        entry = malloc(sizeof(bitcask_fstats_entry));
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
        bitcask_keydir_entry_head* par = GET_ENTRY_LIST_POINTER(entry);
        h = MURMUR_HASH(par->key, par->key_sz, 42);
    }
    else
    {
        h = MURMUR_HASH(entry->key, entry->key_sz, 42);
    }
    return h;
}


static khint_t keydir_entry_equal(bitcask_keydir_entry* lhs,
                                  bitcask_keydir_entry* rhs)
{
    char* lkey;
    char* rkey;
    int lsz, rsz;

    if (IS_ENTRY_LIST(lhs)) {
        bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(lhs);
        lkey = &h->key[0];
        lsz = h->key_sz;
    }
    else
    {
        lkey = &lhs->key[0];
        lsz = lhs->key_sz;
    }
    if (IS_ENTRY_LIST(rhs)) {
        bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(rhs);
        rkey = &h->key[0];
        rsz = h->key_sz;
    }
    else
    {
        rkey = &rhs->key[0];
        rsz = rhs->key_sz;
    }

    if (lsz != rsz)
    {
        return 0;
    }
    else
    {
        return (memcmp(lkey, rkey, lsz) == 0);
    }
}

static khint_t nif_binary_hash(void* void_bin)
{
    ErlNifBinary * bin =(ErlNifBinary*)void_bin;
    return MURMUR_HASH(bin->data, bin->size, 42);
}

static khint_t nif_binary_entry_equal(bitcask_keydir_entry* lhs,
        void * void_rhs)
{
    char* lkey;
    int lsz;

    if (IS_ENTRY_LIST(lhs)) {
        bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(lhs);
        lkey = &h->key[0];
        lsz = h->key_sz;
    }
    else
    {
        lkey = &lhs->key[0];
        lsz = lhs->key_sz;
    }

    ErlNifBinary * rhs = (ErlNifBinary*)void_rhs;

    if (lsz != rhs->size)
    {
        return 0;
    }
    else
    {
        return (memcmp(lkey, rhs->data, lsz) == 0);
    }
}

static khiter_t get_entries_hash(entries_hash_t *hash, ErlNifBinary* key,
                                 khiter_t* itr_ptr, bitcask_keydir_entry** entry_ptr)
{
    khiter_t itr = kh_get_custom(entries, hash, key, nif_binary_hash,
            nif_binary_entry_equal);

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

static inline int is_sib_tombstone(bitcask_keydir_entry_sib *s)
{
    if (s->file_id == 0xffffffff &&
        s->total_sz == 0xffffffff &&
        s->offset == 0xffffffffffffffff)
    {
        return 1;
    }

    return 0;
}

static inline int is_tombstone(bitcask_keydir_entry_head *h)
{
    return is_sib_tombstone(h->sibs);
}


static int proxy_kd_entry_at_time(bitcask_keydir_entry* old,
        uint32_t time, bitcask_keydir_entry_proxy * ret)
{
    if (!IS_ENTRY_LIST(old))
    {
        ret->file_id = old->file_id;
        ret->total_sz = old->total_sz;
        ret->offset = old->offset;
        ret->tstamp = old->tstamp;
        ret->key_sz = old->key_sz;
        ret->key = old->key;
        ret->entry = old;

        return 1;
    }

    bitcask_keydir_entry_head* head = GET_ENTRY_LIST_POINTER(old);

    //grab the newest sib
    bitcask_keydir_entry_sib* s = head->sibs;

    while (s != NULL)
    {
        // break if we're at the right one or at the last sib
        // this can breaks snapshot isolation, but it's better
        // that dropping data, I think.
        if (time >= s->tstamp || s->next == NULL)
        {
            break;
        }
        s = s->next;
    }

    if (s == NULL || is_sib_tombstone(s))
    {
        return 0;
    }

    ret->file_id = s->file_id;
    ret->total_sz = s->total_sz;
    ret->offset = s->offset;
    ret->tstamp = s->tstamp;

    ret->key_sz = head->key_sz;
    ret->key = head->key;
    ret->entry = (bitcask_keydir_entry*)old;

    return 1;
}

static inline int proxy_kd_entry(bitcask_keydir_entry* old,
        bitcask_keydir_entry_proxy * proxy) {
    return proxy_kd_entry_at_time(old, 0xffffffff, proxy);
}

typedef struct
{
    bitcask_keydir_entry * pending_entry;
    bitcask_keydir_entry * entries_entry;
    // Copy of the value of a regular or list entry for quick access
    bitcask_keydir_entry_proxy proxy;
    entries_hash_t * hash;
    khiter_t itr;
    char found;
    char is_tombstone;
    char is_pending_tombstone;
} find_result;

// Find an entry in the pending or entries keydir and update the hash/itr/entry pointers
// if non-NULL.  If iterating is true, restrict search to the frozen keydir.
static void find_keydir_entry(bitcask_keydir* keydir, ErlNifBinary* key,
        uint32_t tstamp, int iterating, find_result * ret)
{
    // Search pending. If keydir handle used is in iterating mode
    // we want to see a past snapshot instead.
    if (keydir->pending != NULL && !iterating)
    {
        if (get_entries_hash(keydir->pending, key,
                    &ret->itr, &ret->pending_entry))
        {
            ret->hash = keydir->pending;
            ret->entries_entry = NULL;
            ret->found = 1;
            proxy_kd_entry(ret->pending_entry, &ret->proxy);
            ret->is_tombstone = ret->is_pending_tombstone = 
                is_pending_tombstone(ret->pending_entry);
            return;
        }
    }

    ret->pending_entry = NULL;
    ret->is_pending_tombstone = 0;

    // If not in pending, check normal entries
    if (get_entries_hash(keydir->entries, key, &ret->itr, &ret->entries_entry))
    {
        ret->hash = keydir->entries;
        if (proxy_kd_entry_at_time(ret->entries_entry, tstamp, &ret->proxy))
        {
            ret->is_tombstone =
                is_tombstone(GET_ENTRY_LIST_POINTER(ret->entries_entry));
            return;
        }
    }

    ret->entries_entry = NULL;
    ret->hash = NULL;
    ret->found = ret->is_tombstone = ret->is_pending_tombstone = 0;
    return;
}
static void update_kd_entry_list(bitcask_keydir_entry *old,
                                 bitcask_keydir_entry_proxy *new,
                                 uint32_t newest_folder) {
    bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(old);
    bitcask_keydir_entry_sib* new_sib;

    //if we're a write newer than the newest folder, just fold in
    if (newest_folder < h->sibs->tstamp)
    {
        new_sib = h->sibs;

        new_sib->file_id = new->file_id;
        new_sib->total_sz = new->total_sz;
        new_sib->offset = new->offset;
        new_sib->tstamp = new->tstamp;
    }
    else // otherwise make a new sib
    {
        new_sib = malloc(sizeof(bitcask_keydir_entry_sib));

        new_sib->file_id = new->file_id;
        new_sib->total_sz = new->total_sz;
        new_sib->offset = new->offset;
        new_sib->tstamp = new->tstamp;
        new_sib->next = h->sibs;

        h->sibs = new_sib;
    }
 }

static bitcask_keydir_entry* new_kd_entry_list(bitcask_keydir_entry *old,
                                               bitcask_keydir_entry_proxy *new) {
    bitcask_keydir_entry_head* ret;
    bitcask_keydir_entry_sib *old_sib, *new_sib;
    ret = malloc(sizeof(bitcask_keydir_entry_head) + old->key_sz);
    old_sib = malloc(sizeof(bitcask_keydir_entry_sib));
    new_sib = malloc(sizeof(bitcask_keydir_entry_sib));

    //fill in list head, use old since new could be a tombstone
    memcpy(ret->key, old->key, old->key_sz);
    ret->key_sz = old->key_sz;
    ret->sibs = new_sib;

    //make new sib
    new_sib->file_id = new->file_id;
    new_sib->total_sz = new->total_sz;
    new_sib->offset = new->offset;
    new_sib->tstamp = new->tstamp;
    new_sib->next = old_sib;

    //make new sib
    old_sib->file_id = old->file_id;
    old_sib->total_sz = old->total_sz;
    old_sib->offset = old->offset;
    old_sib->tstamp = old->tstamp;
    old_sib->next = NULL;

    return MAKE_ENTRY_LIST_POINTER(ret);
}

// Debugging statements commented out to silence unused fn warnings
/*
static void print_entry_list(bitcask_keydir_entry *e)
{
    bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(e);
    char buf[4096];
    assert(h->key_sz+1 < 4096);
    memcpy(&buf, h->key, h->key_sz);
    buf[h->key_sz] = '\0';

    fprintf(stderr, "entry list %p key: %s keylen %d\n",
            h, buf, h->key_sz);

    int sib_count = 0;

    bitcask_keydir_entry_sib
        *s = h->sibs;
    while (s != NULL) {
        fprintf(stderr, "sib %d \n\t%u\t\t%u\n\t%llu\t\t%u\n\n",
                sib_count, s->file_id, s->total_sz, s->offset, s->tstamp);
        sib_count++;
        s = s->next;
        if( s == NULL )
            break;
    }
}

static void print_entry(bitcask_keydir_entry *e)
{
    fprintf(stderr, "entry %p key: %x keylen %d\n",
            e, e->key+4, e->key_sz);

    fprintf(stderr, "\n\t%u\t\t%u\n\t%llu\t\t%u\n\n",
            e->file_id, e->total_sz, e->offset, e->tstamp);
}

static void print_keydir(bitcask_keydir* keydir)
{
    khiter_t itr;
    bitcask_keydir_entry* current_entry;
    fprintf(stderr, "printing keydir: %s size %d\n\n", keydir->name,
            kh_size(keydir->entries));
    // should likely dump some useful stuff here, but don't need it
    // right now
    fprintf(stderr, "entries:\n");
    for (itr = kh_begin(keydir->entries);
         itr != kh_end(keydir->entries);
         ++itr)
    {

        if (kh_exist(keydir->entries, itr))
        {
            current_entry = kh_key(keydir->entries, itr);
            if (IS_ENTRY_LIST(current_entry))
            {
                print_entry_list(current_entry);
            }
            else
            {
                print_entry(current_entry);
            }
        }
    }
    fprintf(stderr, "\npending:\n");
    if (keydir->pending == NULL)
    {
        fprintf(stderr, "NULL\n");
    }
    else
    {
        for (itr = kh_begin(keydir->pending);
             itr != kh_end(keydir->pending);
             ++itr)
        {

            if (kh_exist(keydir->pending, itr))
            {
                current_entry = kh_key(keydir->pending, itr);
                if (IS_ENTRY_LIST(current_entry))
            {
                print_entry_list(current_entry);
            }
                else
                {
                    print_entry(current_entry);
                }
            }
        }
    }
}
*/

static void free_entry_list(bitcask_keydir_entry* e)
{
    bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(e);

    bitcask_keydir_entry_sib
        *temp = NULL,
        *s = h->sibs;
    while (s != NULL) {
        temp = s;
        s = s->next;

        free(temp);
    }

    free(h);
}

static void free_entry(bitcask_keydir_entry *e)
{
    if (IS_ENTRY_LIST(e))
    {
        free_entry_list(e);
    }
    else
    {
        free(e);
    }
}

// Allocate, populate and add entry to the keydir hash based on the key and entry structure
// never need to add an entry list, can update to it later.
static bitcask_keydir_entry* add_entry(bitcask_keydir* keydir,
                                       entries_hash_t* hash,
                                       bitcask_keydir_entry_proxy * entry)
{
    bitcask_keydir_entry* new_entry = malloc(sizeof(bitcask_keydir_entry) +
                                             entry->key_sz);
    new_entry->file_id = entry->file_id;
    new_entry->total_sz = entry->total_sz;
    new_entry->offset = entry->offset;
    new_entry->tstamp = entry->tstamp;
    new_entry->key_sz = entry->key_sz;
    memcpy(new_entry->key, entry->key, entry->key_sz);
    kh_put_set(entries, hash, new_entry);

    return new_entry;
}


static void update_regular_entry(bitcask_keydir_entry* cur_entry,
        bitcask_keydir_entry_proxy* upd_entry)
{
    cur_entry->file_id = upd_entry->file_id;
    cur_entry->total_sz = upd_entry->total_sz;
    cur_entry->offset = upd_entry->offset;
    cur_entry->tstamp = upd_entry->tstamp;
}

// Update the current entry (not from pending) with newer information
static void update_entry(bitcask_keydir* keydir,
                         bitcask_keydir_entry* cur_entry,
                         bitcask_keydir_entry_proxy* upd_entry)
{
    int is_entry_list = IS_ENTRY_LIST(cur_entry);
    int iterating = keydir->keyfolders > 0;

    if (iterating)
    {
        if (is_entry_list)
        {
            // Add to list of values during iteration
            update_kd_entry_list(cur_entry, upd_entry, keydir->newest_folder);
        }
        else
        {
            // Convert regular entry to list during iteration
            khiter_t itr = kh_get(entries, keydir->entries, cur_entry);
            kh_key(keydir->entries, itr) =
                new_kd_entry_list(cur_entry, upd_entry);
            free(cur_entry);
        }
    }
    else // not iterating, so end up with regular entries only.
    {
        if (is_entry_list)
        {
            // Convert list to regular entry
            khiter_t itr = kh_get(entries, keydir->entries, cur_entry);
            bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(cur_entry);

            bitcask_keydir_entry* new_entry =
                malloc(sizeof(bitcask_keydir_entry) +
                       h->key_sz);
            new_entry->file_id = upd_entry->file_id;
            new_entry->total_sz = upd_entry->total_sz;
            new_entry->offset = upd_entry->offset;
            new_entry->tstamp = upd_entry->tstamp;
            new_entry->key_sz = h->key_sz;
            memcpy(new_entry->key, h->key, h->key_sz);
            kh_key(keydir->entries, itr) = new_entry;

            free_entry_list(cur_entry);
        }
        else // regular entry, no iteration
        {
            update_regular_entry(cur_entry, upd_entry);
        }
    }
}

static void remove_entry(bitcask_keydir* keydir, khiter_t itr)
{
    bitcask_keydir_entry * entry = kh_key(keydir->entries, itr);
    kh_del(entries, keydir->entries, itr);
    free_entry(entry);
}

static void set_entry_tombstone(bitcask_keydir* keydir, khiter_t itr,
                         uint32_t remove_time)
{
    bitcask_keydir_entry_proxy tombstone;
    tombstone.tstamp = remove_time;
    tombstone.offset = 0xffffffffffffffff;
    tombstone.total_sz = 0xffffffff;
    tombstone.file_id = 0xffffffff;
    tombstone.key_sz = 0;

    bitcask_keydir_entry * entry= kh_key(keydir->entries, itr);
    if (!IS_ENTRY_LIST(entry))
    {
        // update into an entry list
        bitcask_keydir_entry* new_entry_list;
        new_entry_list = new_kd_entry_list(entry, &tombstone);
        kh_key(keydir->entries, itr) = new_entry_list;
    }
    else
    {
        //need to update the entry list with a tombstone
        update_kd_entry_list(entry, &tombstone, keydir->newest_folder);
    }
}

static void put_entry(bitcask_keydir * keydir, find_result * r,
        bitcask_keydir_entry_proxy * entry)
{
    // found in pending (keydir is frozen), update that one
    if (r->pending_entry)
    {
        update_regular_entry(r->pending_entry, entry);
    }
    // iterating (frozen) and not found in pending, add to pending
    else if (keydir->pending)
    {
        add_entry(keydir, keydir->pending, entry);
        keydir->pending_updated++;
    }
    // found in entries, update that one
    else if (r->entries_entry)
    {
        update_entry(keydir, r->entries_entry, entry);
    }
    // Not found and not iterating, add to entries
    else
    {
        add_entry(keydir, keydir->entries, entry);
    }

    if (entry->file_id > keydir->biggest_file_id)
    {
        keydir->biggest_file_id = entry->file_id;
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_put_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    bitcask_keydir_entry_proxy entry;
    ErlNifBinary key;
    uint32_t newest_put;
    uint32_t old_file_id;
    uint64_t old_offset;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint(env, argv[2], &(entry.file_id)) &&
        enif_get_uint(env, argv[3], &(entry.total_sz)) &&
        enif_get_uint64_bin(env, argv[4], &(entry.offset)) &&
        enif_get_uint(env, argv[5], &(entry.tstamp)) &&
        enif_get_uint(env, argv[6], &(newest_put)) &&
        enif_get_uint(env, argv[7], &(old_file_id)) &&
        enif_get_uint64_bin(env, argv[8], &(old_offset)))
    {
        bitcask_keydir* keydir = handle->keydir;
        entry.key = (char*)key.data;
        entry.key_sz = key.size;
        entry.entry = NULL;

        LOCK(keydir);

        DEBUG("+++ Put file_id=%d offset=%d total_sz=%d\r\n",
              (int) entry.file_id, (int) entry.offset,
              (int)entry.total_sz);

        // Check for put on a new key or updating a pending tombstone
        find_result f;
        find_keydir_entry(keydir, &key, (uint32_t)-1, 0, &f);

        // If conditional put and not found, bail early
        if (!f.found && old_file_id != 0)
        {
            UNLOCK(keydir);
            return ATOM_ALREADY_EXISTS;
        }

        // If put would resize and iterating, start pending hash
        if (kh_put_will_resize(entries, keydir->entries) &&
            keydir->keyfolders != 0 &&
            (keydir->pending == NULL))
        {
            keydir->pending = kh_init(entries);
            keydir->pending_start = time(NULL);
        }

        if (!f.found)
        {
            keydir->key_count++;
            keydir->key_bytes += key.size;

            // Increment live and total stats.
            update_fstats(env, keydir, entry.file_id, entry.tstamp,
                          1, 1, entry.total_sz, entry.total_sz);

            put_entry(keydir, &f, &entry);

            UNLOCK(keydir);
            return ATOM_OK;
        }

        // If conditional put and no match, bail
        if (old_file_id != 0 &&
            !(old_file_id == f.proxy.file_id &&
              old_offset == f.proxy.offset))
        {
            UNLOCK(keydir);
            return ATOM_ALREADY_EXISTS;
        }

        // Avoid updating with stale data. Allow if:
        // - If real put to current write file, not a stale one
        // - If internal put (from merge, etc) with newer timestamp
        // - If internal put with a higher file id or higher offset
        if ((newest_put &&
             (entry.file_id >= keydir->biggest_file_id)) ||
            (! newest_put &&
             (f.proxy.tstamp < entry.tstamp)) ||
            (! newest_put &&
             ((f.proxy.file_id < entry.file_id) ||
              (((f.proxy.file_id == entry.file_id) &&
                (f.proxy.offset < entry.offset))))))
        {
            // Remove the stats for the old entry and add the new
            if (f.proxy.file_id != entry.file_id) // different files
            {
                update_fstats(env, keydir, f.proxy.file_id, 0,
                              -1, 0,
                              -f.proxy.total_sz, 0);
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              1, 1,
                              entry.total_sz, entry.total_sz);
            }
            else // file_id is same, change live/total in one entry
            {
                update_fstats(env, keydir, entry.file_id, 0,
                              0, 1,
                              entry.total_sz - f.proxy.total_sz,
                              entry.total_sz);
            }

            put_entry(keydir, &f, &entry);

            UNLOCK(keydir);
            return ATOM_OK;
        }
        else
        {
            // If not live yet, live stats are not updated, but total stats are
            if (!keydir->is_ready)
            {
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              0, 1, 0, entry.total_sz);
            }
            UNLOCK(keydir);
            return ATOM_ALREADY_EXISTS;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

/* int erts_printf(const char *, ...); */

ERL_NIF_TERM bitcask_nifs_keydir_get_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    ErlNifBinary key;
    uint32_t time;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint(env, argv[2], &time))
    {
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);

        DEBUG("+++ Get issued\r\n");


        find_result f;
        find_keydir_entry(keydir, &key, time, handle->iterating, &f);

        if (f.found)
        {
            ERL_NIF_TERM result;
            result = enif_make_tuple6(env,
                                      ATOM_BITCASK_ENTRY,
                                      argv[1], /* Key */
                                      enif_make_uint(env, f.proxy.file_id),
                                      enif_make_uint(env, f.proxy.total_sz),
                                      enif_make_uint64_bin(env,
                                                           f.proxy.offset),
                                      enif_make_uint(env, f.proxy.tstamp));
            DEBUG(" ... returned value\r\n");
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
    uint32_t tstamp;
    uint32_t file_id;
    uint64_t offset;
    uint32_t remove_time;
    // If this call has 6 arguments, this is a conditional removal. We
    // only want to actually remove the entry if the tstamp, fileid and
    // offset matches the one provided. A sort of poor-man's CAS.
    int is_conditional = argc == 6;
    int common_args_ok =
        enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key);
    int other_args_ok =
        is_conditional ?
        (enif_get_uint(env, argv[2], (unsigned int*)&tstamp) &&
         enif_get_uint(env, argv[3], (unsigned int*)&file_id) &&
         enif_get_uint64_bin(env, argv[4], (uint64_t*)&offset) &&
         enif_get_uint(env, argv[5], &remove_time))
        :
        ( enif_get_uint(env, argv[2], &remove_time));

    if (common_args_ok && other_args_ok)
    {
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);

        DEBUG("+++ Remove\r\n");

        find_result fr;
        find_keydir_entry(keydir, &key, remove_time, 0, &fr);

        if (fr.found)
        {
            // If a conditional remove, bail if not a match.
            int cond_no_match = is_conditional &&
                (fr.proxy.tstamp != tstamp || fr.proxy.file_id != file_id ||
                 fr.proxy.offset != offset);

            if (cond_no_match || fr.is_pending_tombstone)
            {
                UNLOCK(keydir);
                return ATOM_OK;
            }

            // Remove the key from the keydir stats
            keydir->key_count--;
            keydir->key_bytes -= fr.proxy.key_sz;

            // Remove from file stats
            update_fstats(env, keydir, fr.proxy.file_id, fr.proxy.tstamp,
                          -1, 0, -fr.proxy.total_sz, 0);

            // If found an entry in the pending hash, convert it to a tombstone
            if (fr.pending_entry)
            {
                set_pending_tombstone(fr.pending_entry);
            }
            // If frozen, add tombstone to pending hash (iteration must have
            // started between put/remove call in bitcask:delete.
            else if (keydir->pending)
            {
                bitcask_keydir_entry* pending_entry =
                    add_entry(keydir, keydir->pending, &fr.proxy);
                set_pending_tombstone(pending_entry);
            }
            // If not iterating, just remove.
            else if(keydir->keyfolders == 0)
            { 
                remove_entry(keydir, fr.itr);
            }
            // else found in entries while iterating
            else
            {
                set_entry_tombstone(keydir, fr.itr, remove_time);
            }
        } // if found

        UNLOCK(keydir);
        return ATOM_OK;;
    } // if args OK

    return enif_make_badarg(env);
}

bitcask_keydir_entry * clone_entry(bitcask_keydir_entry * curr)
{
    if (IS_ENTRY_LIST(curr))
    {
        bitcask_keydir_entry_head * curr_head = GET_ENTRY_LIST_POINTER(curr);
        size_t head_sz = sizeof(bitcask_keydir_entry_head) + curr->key_sz;
        bitcask_keydir_entry_head * new_head = malloc(head_sz);
        memcpy(new_head, curr_head, head_sz);
        bitcask_keydir_entry_sib ** sib_ptr = &new_head->sibs;
        bitcask_keydir_entry_sib * next_sib = curr_head->sibs;
        while (next_sib)
        {
            bitcask_keydir_entry_sib * sib =
                malloc(sizeof(bitcask_keydir_entry_sib));
            memcpy(sib, next_sib, sizeof(bitcask_keydir_entry_sib));
            *sib_ptr = sib;
            sib_ptr = &sib->next;
            next_sib = next_sib->next;
        }
        *sib_ptr = NULL;
        return MAKE_ENTRY_LIST_POINTER(new_head); 
    }
    else
    {
        size_t new_sz = sizeof(bitcask_keydir_entry) + curr->key_sz;
        bitcask_keydir_entry* new = malloc(new_sz);
        memcpy(new, curr, new_sz);
        return curr;
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
        bitcask_keydir* new_keydir = malloc(sizeof(bitcask_keydir));
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
                bitcask_keydir_entry* new = clone_entry(curr);
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
                    bitcask_keydir_entry* new = clone_entry(curr);
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
                bitcask_fstats_entry* new_f = malloc(sizeof(bitcask_fstats_entry));
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

ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        uint32_t ts;
        int maxage;
        int maxputs;

        LOCK(handle->keydir);
        DEBUG("+++ itr\r\n");
        bitcask_keydir* keydir = handle->keydir;

        // If a iterator thread is already active for this keydir, bail
        if (handle->iterating)
        {
            UNLOCK(handle->keydir);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_IN_PROCESS);
        }

        if (!(enif_get_uint(env, argv[1], &ts) &&
              enif_get_int(env, argv[2], (int*)&maxage) &&
              enif_get_int(env, argv[3], (int*)&maxputs)))
        {
            UNLOCK(handle->keydir);
            return enif_make_badarg(env);
        }

        if (can_itr_keydir(keydir, ts, maxage, maxputs))
        {
            handle->iterating = 1;
            handle->timestamp = ts;
            keydir->newest_folder = ts;
            keydir->keyfolders++;
            handle->iterator = kh_begin(keydir->entries);
            UNLOCK(handle->keydir);
            return ATOM_OK;
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
                    keydir->pending_awaken = malloc(size);
                }
                else
                {
                    keydir->pending_awaken = realloc(keydir->pending_awaken, size);
                }
            }
            enif_self(env, &keydir->pending_awaken[keydir->pending_awaken_count]);
            keydir->pending_awaken_count++;
            UNLOCK(handle->keydir);
            return ATOM_OUT_OF_DATE;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        bitcask_keydir* keydir = handle->keydir;
        int locked = 0;

        if (handle->iterating != 1)
        {
            // Iteration not started!
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_NOT_STARTED);
        }

        if (keydir->pending != NULL)
        {
            //no need to grab the lock if the keydir has frozen
            //need to complete as quickly as possible.
            LOCK(keydir);
            locked = 1;
        }

        while (handle->iterator != kh_end(keydir->entries))
        {
            if (kh_exist(keydir->entries, handle->iterator))
            {
                bitcask_keydir_entry* entry = kh_key(keydir->entries, handle->iterator);
                ErlNifBinary key;
                bitcask_keydir_entry_proxy proxy;

                if (!proxy_kd_entry_at_time(entry, handle->timestamp, &proxy))
                {
                    (handle->iterator)++;
                    continue;
                }

                // Alloc the binary and make sure it succeeded
                if (!enif_alloc_binary_compat(env, proxy.key_sz, &key))
                {
                    if (locked)
                    {
                        UNLOCK(keydir);
                    }
                    return ATOM_ALLOCATION_ERROR;
                }

                // Copy the data from our key to the new allocated binary
                // TODO: If we maintained a ErlNifBinary in the original entry, could we
                // get away with not doing a copy here?
                memcpy(key.data, proxy.key, proxy.key_sz);
                ERL_NIF_TERM curr = enif_make_tuple6(env,
                                                     ATOM_BITCASK_ENTRY,
                                                     enif_make_binary(env, &key),
                                                     enif_make_uint(env, proxy.file_id),
                                                     enif_make_uint(env, proxy.total_sz),
                                                     enif_make_uint64_bin(env, proxy.offset),
                                                     enif_make_uint(env, proxy.tstamp));

                // Update the iterator to the next entry
                (handle->iterator)++;
                if (locked)
                {
                    UNLOCK(keydir);
                }
                return curr;
            }
            else
            {
                // No item in this slot; increment the iterator and keep looping
                (handle->iterator)++;
            }
        }

        if (locked)
        {
            UNLOCK(keydir);
        }
        // The iterator is at the end of the table
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

        if (handle->keydir->keyfolders == 0 && handle->keydir->pending != NULL)
        {
            merge_pending_entries(env, handle->keydir);
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
                             enif_make_uint64(env, keydir->iter_generation),
                             enif_make_ulong(env, keydir->keyfolders),
                             keydir->pending == NULL ? ATOM_FALSE : ATOM_TRUE);

        ERL_NIF_TERM result = enif_make_tuple4(env,
                                               enif_make_uint(env, keydir->key_count),
                                               enif_make_uint(env, keydir->key_bytes),
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
        /* Using PULSE_SEND here sometimes deadlocks the Bitcask PULSE test.
           Reverting to using enif_send for now.
           TODO: Check if PULSE_SEND is really necessary and investigate/fix
                 deadlock in the future
        */
        /* PULSE_SEND(env, &keydir->pending_awaken[idx], msg_env, msg); */
        enif_send(env, &keydir->pending_awaken[idx], msg_env, msg);
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
                    free(pending_entry);
                }
                /* entries: empty, pending:value */
                else
                {
                    // Move to entries, do not free
                    kh_put_set(entries, keydir->entries, pending_entry);
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
                    remove_entry(keydir, ent_itr);
                    free(pending_entry);
                }
                /* entries: present, pending:value */
                else
                {
                    free_entry(entries_entry);
                    kh_key(keydir->entries, ent_itr) = pending_entry;
                }
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
        free(keydir->pending_awaken);
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

static void free_keydir(bitcask_keydir* keydir)
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
            free_entry(current_entry);
        }
    }

    kh_destroy(entries, keydir->entries);

    bitcask_fstats_entry* curr_f;

    for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
    {
        if (kh_exist(keydir->fstats, itr))
        {
            curr_f = kh_val(keydir->fstats, itr);
            free(curr_f);
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

        enif_mutex_lock(keydir->mutex);
        keydir->refcount--;
        if (keydir->refcount == 0)
        {
            // This is the last reference to the named keydir. As such,
            // remove it from the hashtable so no one else tries to use it
            khiter_t itr = kh_get(global_keydirs, priv->global_keydirs, keydir->name);
            kh_del(global_keydirs, priv->global_keydirs, itr);
            enif_mutex_unlock(keydir->mutex);
        }
        else
        {
            enif_mutex_unlock(keydir->mutex);
            // At least one other reference; just throw away our keydir pointer
            // so the check below doesn't release the memory.
            keydir = 0;
        }

        // Unlock ASAP. Wanted to avoid holding this mutex while we clean up the
        // keydir, since it may take a while to walk a large keydir and free each
        // entry.
        enif_mutex_unlock(priv->global_keydirs_lock);
    }

    // If keydir is still defined, it's either privately owned or has a
    // refcount of 0. Either way, we want to release it.
    if (keydir)
    {
        if (keydir->mutex)
        {
            enif_mutex_destroy(keydir->mutex);
        }

        free_keydir(keydir);
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
    bitcask_priv_data* priv = malloc(sizeof(bitcask_priv_data));
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
    ATOM_ITERATION_IN_PROCESS = enif_make_atom(env, "iteration_in_process");
    ATOM_ITERATION_NOT_PERMITTED = enif_make_atom(env, "iteration_not_permitted");
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
