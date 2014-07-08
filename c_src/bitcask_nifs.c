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
#include <sys/time.h>
#include <assert.h>

#include "erl_nif.h"
#include "erl_driver.h"
#include "erl_nif_compat.h"
#include "erl_nif_util.h"

#include "khash.h"
#include "murmurhash.h"

#include <stdio.h>

//typesystem hack to avoid some incorrect errors.
typedef ErlNifUInt64 uint64;

#ifdef BITCASK_DEBUG
#include <stdarg.h>
#include <ctype.h>
#include <string.h>
void DEBUG(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}
int erts_snprintf(char *, size_t, const char *, ...); 
#define MAX_DEBUG_STR 128
#define DEBUG_STR(N, V) \
    char N[MAX_DEBUG_STR];\
    erts_snprintf(N, MAX_DEBUG_STR, "%s", V)

#define DEBUG_BIN(N, V, S) \
    char N[MAX_DEBUG_STR];\
    format_bin(N, MAX_DEBUG_STR, (unsigned char*)V, (size_t)S)

#define DEBUG2 DEBUG
#else
void DEBUG2(const char *fmt, ...) { }
#define DEBUG_STR(A, B)
#define DEBUG_BIN(N, V, S)
#  define DEBUG(X, ...) {}
#endif

#if defined(BITCASK_DEBUG) && defined(BITCASK_DEBUG_KEYDIR)
#  define DEBUG_KEYDIR(KD) print_keydir((KD))
#  define DEBUG_ENTRY(E) print_entry((E))
#else
#  define DEBUG_KEYDIR(X) {}
#  define DEBUG_ENTRY(E) {}
#endif

#ifdef PULSE
#include "pulse_c_send.h"
#endif

#ifdef BITCASK_DEBUG
void format_bin(char * buf, size_t buf_size, const unsigned char * bin, size_t bin_size)
{
    char cbuf[4]; // up to 3 digits + \0
    int is_printable = 1;
    int i, n;
    size_t av_size = buf_size;

    for (i=0;i<bin_size;++i)
    {
        if (!isprint(bin[i]))
        {
            is_printable = 0;
            break;
        }
    }

    buf[0] = '\0';

    // TODO: Protect against overriding that buffer yo!
    if (is_printable)
    {
        strcat(buf, "<<\"");
        av_size -= 3;
        n = av_size < bin_size ? av_size : bin_size;
        strncat(buf, (char*)bin, n);
        strcat(buf, "\">>");
    }
    else
    {
        strcat(buf, "<<");
        for (i=0;i<bin_size;++i)
        {
            if (i>0)
            {
                strcat(buf, ",");
            }
            sprintf(cbuf, "%u", bin[i]);
            strcat(buf, cbuf);
        }
        strcat(buf, ">>");
    }

}
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
    uint64_t epoch;
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
    uint64_t expiration_epoch; // file obsolete at this epoch
} bitcask_fstats_entry;

struct bitcask_keydir_entry_sib
{
    uint32_t file_id;
    uint32_t total_sz;
    uint64_t offset;
    uint64_t epoch;
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

// An entry pointer may be tagged to indicate it really points to an
// list of entries with different timestamps. Those are created when
// there are concurrent iterations and updates.
#define IS_ENTRY_LIST(p) ((uint64_t)p&1)
#define GET_ENTRY_LIST_POINTER(p) ((bitcask_keydir_entry_head*)((uint64_t)p&(uint64_t)~1))
#define MAKE_ENTRY_LIST_POINTER(p) ((bitcask_keydir_entry*)((uint64_t)p|(uint64_t)1))

// Holds values fetched from a regular entry or a snapshot from an entry list.
typedef struct
{
    uint32_t file_id;
    uint32_t total_sz;
    uint64_t epoch;
    uint64_t offset;
    uint32_t tstamp;
    uint16_t is_tombstone;
    uint16_t key_sz;
    char *   key;
} bitcask_keydir_entry_proxy;

#define MAX_TIME ((uint32_t)-1)
#define MAX_EPOCH ((uint64_t)-1)
#define MAX_SIZE ((uint32_t)-1)
#define MAX_FILE_ID ((uint32_t)-1)
#define MAX_OFFSET ((uint64_t)-1)

KHASH_MAP_INIT_INT(fstats, bitcask_fstats_entry*);

typedef khash_t(entries) entries_hash_t;
typedef khash_t(fstats) fstats_hash_t;

typedef struct
{
    // The hash where entries are usually stored. It may contain
    // regular entries or entry lists created during keyfolding.
    entries_hash_t* entries;
    // Hash used when it's not possible to update entries without
    // resizing it, which would break ongoing keyfolder on it.
    // It can only contain regular entries, not entry lists.
    entries_hash_t* pending;
    fstats_hash_t*  fstats;
    uint64_t      epoch;
    uint64_t      key_count;
    uint64_t      key_bytes;
    uint32_t      biggest_file_id;
    unsigned int  refcount;
    unsigned int  keyfolders;
    uint64_t      newest_folder;  // Epoch for newest folder
    uint64_t      iter_generation;
    char          iter_mutation;         // Mutation while iterating?
    uint64_t      sweep_last_generation; // iter_generation of last sibling sweep
    khiter_t      sweep_itr;             // iterator for sibling sweep
    uint64_t      pending_updated;
    uint64_t      pending_start_time;  // UNIX epoch seconds (since 1970)
    uint64_t      pending_start_epoch;
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
    uint64_t        epoch;
} bitcask_keydir_handle;

typedef struct
{
    int   fd;
    int   is_write_lock;
    char  filename[0];
} bitcask_lock_handle;

KHASH_INIT(global_biggest_file_id, char*, uint32_t, 1, kh_str_hash_func, kh_str_hash_equal);
KHASH_INIT(global_keydirs, char*, bitcask_keydir*, 1, kh_str_hash_func, kh_str_hash_equal);

typedef struct
{
    khash_t(global_biggest_file_id)* global_biggest_file_id;
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

// Related to tombstones in the pending hash.
// Notice that tombstones in the entries hash are different.
#define is_pending_tombstone(e) ((e)->offset == MAX_OFFSET)
#define set_pending_tombstone(e) {(e)->offset = MAX_OFFSET; }

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
static ERL_NIF_TERM ATOM_UNDEFINED;
static ERL_NIF_TERM ATOM_EOF;
static ERL_NIF_TERM ATOM_CREATE;
static ERL_NIF_TERM ATOM_READONLY;
static ERL_NIF_TERM ATOM_O_SYNC;
// lseek equivalents for file_position
static ERL_NIF_TERM ATOM_CUR;
static ERL_NIF_TERM ATOM_BOF;

// Prototypes
ERL_NIF_TERM bitcask_nifs_keydir_new0(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_new1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_maybe_keydir_new1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_mark_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_get_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_get_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_put_int(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_copy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_trim_fstats(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM bitcask_nifs_increment_file_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

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
ERL_NIF_TERM bitcask_nifs_file_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM bitcask_nifs_update_fstats(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_set_pending_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM errno_atom(ErlNifEnv* env, int error);
ERL_NIF_TERM errno_error_tuple(ErlNifEnv* env, ERL_NIF_TERM key, int error);

static void merge_pending_entries(ErlNifEnv* env, bitcask_keydir* keydir);
static void lock_release(bitcask_lock_handle* handle);

static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg);
static void bitcask_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg);

static ErlNifFunc nif_funcs[] =
{
#ifdef PULSE
    {"set_pulse_pid", 1, set_pulse_pid},
#endif
    {"keydir_new", 0, bitcask_nifs_keydir_new0},
    {"keydir_new", 1, bitcask_nifs_keydir_new1},
    {"maybe_keydir_new", 1, bitcask_nifs_maybe_keydir_new1},
    {"keydir_mark_ready", 1, bitcask_nifs_keydir_mark_ready},
    {"keydir_put_int", 10, bitcask_nifs_keydir_put_int},
    {"keydir_get_int", 3, bitcask_nifs_keydir_get_int},
    {"keydir_get_epoch", 1, bitcask_nifs_keydir_get_epoch},
    {"keydir_remove", 3, bitcask_nifs_keydir_remove},
    {"keydir_remove_int", 6, bitcask_nifs_keydir_remove},
    {"keydir_copy", 1, bitcask_nifs_keydir_copy},
    {"keydir_itr_int", 4, bitcask_nifs_keydir_itr},
    {"keydir_itr_next_int", 1, bitcask_nifs_keydir_itr_next},
    {"keydir_itr_release", 1, bitcask_nifs_keydir_itr_release},
    {"keydir_info", 1, bitcask_nifs_keydir_info},
    {"keydir_release", 1, bitcask_nifs_keydir_release},
    {"keydir_trim_fstats", 2, bitcask_nifs_keydir_trim_fstats},

    {"increment_file_id", 1, bitcask_nifs_increment_file_id},
    {"increment_file_id", 2, bitcask_nifs_increment_file_id},

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
    {"file_seekbof_int", 1, bitcask_nifs_file_seekbof},
    {"file_truncate_int", 1, bitcask_nifs_file_truncate},
    {"update_fstats", 8, bitcask_nifs_update_fstats},
    {"set_pending_delete", 2, bitcask_nifs_set_pending_delete}
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

ERL_NIF_TERM bitcask_nifs_maybe_keydir_new1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1))
    {
        // Get our private stash and check the global hash table for this entry
        bitcask_priv_data* priv = (bitcask_priv_data*)enif_priv_data(env);
        
        enif_mutex_lock(priv->global_keydirs_lock);
        khiter_t itr = kh_get(global_keydirs, priv->global_keydirs, name);
        khiter_t table_end = kh_end(priv->global_keydirs); /* get end while lock is held! */
        enif_mutex_unlock(priv->global_keydirs_lock);
        if (itr != table_end)
        {
            return bitcask_nifs_keydir_new1(env, argc, argv);
        } 
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_NOT_READY);
        }
    } 
    else 
    {
        return enif_make_badarg(env);
    }
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

            khiter_t itr_biggest_file_id = kh_get(global_biggest_file_id, priv->global_biggest_file_id, name);
            if (itr_biggest_file_id != kh_end(priv->global_biggest_file_id)) {
                uint32_t old_biggest_file_id = kh_val(priv->global_biggest_file_id, itr_biggest_file_id);
                keydir->biggest_file_id = old_biggest_file_id;
            }
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
                          uint64_t expiration_epoch,
                          int32_t live_increment, int32_t total_increment,
                          int32_t live_bytes_increment, int32_t total_bytes_increment,
                          int32_t should_create)
{
    bitcask_fstats_entry* entry = 0;
    khiter_t itr = kh_get(fstats, keydir->fstats, file_id);

    if (itr == kh_end(keydir->fstats))
    {
        if (!should_create)
        {
            return;
        }

        // Need to initialize new entry and add to the table
        entry = malloc(sizeof(bitcask_fstats_entry));
        memset(entry, '\0', sizeof(bitcask_fstats_entry));
        entry->expiration_epoch = MAX_EPOCH;
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

    if (expiration_epoch < entry->expiration_epoch)
    {
        entry->expiration_epoch = expiration_epoch;
    }

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

// NIF wrapper around update_fstats().
ERL_NIF_TERM bitcask_nifs_update_fstats(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    uint32_t file_id, tstamp;
    int32_t live_increment, total_increment;
    int32_t live_bytes_increment, total_bytes_increment;
    int32_t should_create;

    if (argc == 8
            && enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE,
                (void**)&handle)
            && enif_get_uint(env, argv[1], &file_id)
            && enif_get_uint(env, argv[2], &tstamp)
            && enif_get_int(env, argv[3], &live_increment)
            && enif_get_int(env, argv[4], &total_increment)
            && enif_get_int(env, argv[5], &live_bytes_increment)
            && enif_get_int(env, argv[6], &total_bytes_increment)
            && enif_get_int(env, argv[7], &should_create))
    {
        LOCK(handle->keydir);
        update_fstats(env, handle->keydir, file_id, tstamp, MAX_EPOCH,
                live_increment, total_increment,
                live_bytes_increment, total_bytes_increment,
                should_create);
        UNLOCK(handle->keydir);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_set_pending_delete(ErlNifEnv* env, int argc,
        const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    uint32_t file_id;

    if (argc == 2
            && enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE,
                (void**)&handle)
            && enif_get_uint(env, argv[1], &file_id))
    {
        LOCK(handle->keydir);
        update_fstats(env, handle->keydir, file_id, 0, handle->keydir->epoch,
                0, 0, 0, 0, 0);
        UNLOCK(handle->keydir);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
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

// Custom hash function to be able to look up entries using a
// ErlNifBinary without allocating a new entry just for that.
static khint_t nif_binary_hash(void* void_bin)
{
    ErlNifBinary * bin =(ErlNifBinary*)void_bin;
    return MURMUR_HASH(bin->data, bin->size, 42);
}

// Custom equals function to be able to look up entries using a
// ErlNifBinary without allocating a new entry just for that.
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
    if (s->file_id == MAX_TIME &&
        s->total_sz == MAX_SIZE &&
        s->offset == MAX_OFFSET)
    {
        return 1;
    }

    return 0;
}

// Extracts the entry values from a regular entry or from the 
// closest snapshot in time in an entry list.
static int proxy_kd_entry_at_epoch(bitcask_keydir_entry* old,
                                   uint64_t epoch, bitcask_keydir_entry_proxy * ret)
{
    if (!IS_ENTRY_LIST(old))
    {
        if (epoch < old->epoch)
            return 0;

        ret->file_id = old->file_id;
        ret->total_sz = old->total_sz;
        ret->offset = old->offset;
        ret->tstamp = old->tstamp;
        ret->epoch = old->epoch;
        ret->key_sz = old->key_sz;
        ret->key = old->key;
        ret->is_tombstone = is_pending_tombstone(old);

        return 1;
    }

    bitcask_keydir_entry_head* head = GET_ENTRY_LIST_POINTER(old);

    //grab the newest sib
    bitcask_keydir_entry_sib* s = head->sibs;

    while (s != NULL)
    {
        if (epoch >= s->epoch)
        {
            break;
        }
        s = s->next;
    }

    if (s == NULL)
    {
        return 0;
    }

    ret->file_id = s->file_id;
    ret->total_sz = s->total_sz;
    ret->offset = s->offset;
    ret->tstamp = s->tstamp;
    ret->is_tombstone = is_sib_tombstone(s);
    ret->epoch = s->epoch;

    ret->key_sz = head->key_sz;
    ret->key = head->key;

    return 1;
}

// Extracts entry values from a regular entry or the latest snapshot
// from an entry list.
static inline int proxy_kd_entry(bitcask_keydir_entry* old,
                                 bitcask_keydir_entry_proxy * proxy)
{
    return proxy_kd_entry_at_epoch(old, MAX_EPOCH, proxy);
}

// All info about a lookup with find_keydir_entry.
typedef struct
{
    // Entry found in the pending hash. If set, entries_entry will be NULL.
    bitcask_keydir_entry * pending_entry;
    // Entry found in the entries hash. If set, pending_entry is NULL
    bitcask_keydir_entry * entries_entry;
    // Copy of the values of the found entry, if any, whether it's
    // a regular entry or list.
    bitcask_keydir_entry_proxy proxy;
    // Hash (entries or pending) where the entry was found.
    entries_hash_t * hash;
    khiter_t itr;
    // True if found, even if it is a tombstone
    char found;
} find_result;

// Find an entry in the pending hash when they keydir is frozen, or in the
// entries hash otherwise.
static void find_keydir_entry(bitcask_keydir* keydir, ErlNifBinary* key,
                              uint64_t epoch, find_result * ret)
{
    // Search pending. If keydir handle used is in iterating mode
    // we want to see a past snapshot instead.
    if (keydir->pending != NULL)
    {
        if (get_entries_hash(keydir->pending, key,
                             &ret->itr, &ret->pending_entry)
            && (epoch >= ret->pending_entry->epoch))
        {
            DEBUG("Found in pending %llu > %llu", epoch, ret->pending_entry->epoch);
            ret->hash = keydir->pending;
            ret->entries_entry = NULL;
            ret->found = 1;
            proxy_kd_entry(ret->pending_entry, &ret->proxy);
            return;
        }
    }

    // Definitely not in the pending entries
    ret->pending_entry = NULL;

    // If a snapshot for that time is found in regular entries
    if (get_entries_hash(keydir->entries, key, &ret->itr, &ret->entries_entry)
        && proxy_kd_entry_at_epoch(ret->entries_entry, epoch, &ret->proxy))
    {
        ret->hash = keydir->entries;
        ret->found = 1;
        return;
    }

    ret->entries_entry = NULL;
    ret->hash = NULL;
    ret->found = 0;
    return;
}

static void update_kd_entry_list(bitcask_keydir_entry *old,
                                 bitcask_keydir_entry_proxy *new,
                                 int iterating_p) {
    bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(old);
    bitcask_keydir_entry_sib* new_sib;

    if (! iterating_p)
    {
        new_sib = h->sibs;

        new_sib->file_id = new->file_id;
        new_sib->total_sz = new->total_sz;
        new_sib->offset = new->offset;
        new_sib->epoch = new->epoch;
        new_sib->tstamp = new->tstamp;
    }
    else // otherwise make a new sib
    {
        new_sib = malloc(sizeof(bitcask_keydir_entry_sib));

        new_sib->file_id = new->file_id;
        new_sib->total_sz = new->total_sz;
        new_sib->offset = new->offset;
        new_sib->epoch = new->epoch;
        new_sib->tstamp = new->tstamp;
        new_sib->next = h->sibs;

        h->sibs = new_sib;
    }
}

static bitcask_keydir_entry* new_kd_entry_list(bitcask_keydir_entry *old,
                                               bitcask_keydir_entry_proxy *new)
{
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
    new_sib->epoch = new->epoch;
    new_sib->tstamp = new->tstamp;
    new_sib->next = old_sib;

    //make new sib
    old_sib->file_id = old->file_id;
    old_sib->total_sz = old->total_sz;
    old_sib->offset = old->offset;
    old_sib->epoch = old->epoch;
    old_sib->tstamp = old->tstamp;
    old_sib->next = NULL;

    return MAKE_ENTRY_LIST_POINTER(ret);
}

#ifdef BITCASK_DEBUG
void print_entry_list(bitcask_keydir_entry *e)
{
    bitcask_keydir_entry_head* h = GET_ENTRY_LIST_POINTER(e);
    char buf[4096];
    assert(h->key_sz+1 < 4096);
    memcpy(&buf, h->key, h->key_sz);
    buf[h->key_sz] = '\0';

    fprintf(stderr, "entry list %p key: %s keylen %d\r\n",
            h, buf, h->key_sz);

    int sib_count = 0;

    bitcask_keydir_entry_sib
        *s = h->sibs;
    while (s != NULL) {
        fprintf(stderr, "sib %d \r\n\t%u\t\t%u\r\n\t%llu\t\t%u\tepoch=%u\r\n\r\n",
                sib_count, s->file_id, s->total_sz, (unsigned long long)s->offset, s->tstamp, s->epoch);
        sib_count++;
        s = s->next;
        if( s == NULL )
            break;
    }
}

void print_entry(bitcask_keydir_entry *e)
{
    if (IS_ENTRY_LIST(e))
    {
        print_entry_list(e);
        return;
    }

    fprintf(stderr, "entry %p key: %d keylen %d\r\n",
            e, (int)e->key[3], e->key_sz);

    fprintf(stderr, "\r\n\t%u\t\t%u\r\n\t%llu\t\t%u\tepoch=%u\r\n\r\n",
            e->file_id, e->total_sz, (unsigned long long)e->offset, e->tstamp, e->epoch);
}

void print_keydir(bitcask_keydir* keydir)
{
    khiter_t itr;
    bitcask_keydir_entry* current_entry;
    fprintf(stderr, "printing keydir: %s size %d\r\n\r\n", keydir->name,
            kh_size(keydir->entries));
    // should likely dump some useful stuff here, but don't need it
    // right now
    fprintf(stderr, "entries:\r\n");
    for (itr = kh_begin(keydir->entries);
         itr != kh_end(keydir->entries);
         ++itr)
    {

        if (kh_exist(keydir->entries, itr))
        {
            current_entry = kh_key(keydir->entries, itr);
            print_entry(current_entry);
        }
    }
    fprintf(stderr, "\r\npending:\r\n");
    if (keydir->pending == NULL)
    {
        fprintf(stderr, "NULL\r\n");
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
                print_entry(current_entry);
            }
        }
    }
}
#endif

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
    new_entry->epoch = entry->epoch;
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
    cur_entry->epoch = upd_entry->epoch;
    cur_entry->offset = upd_entry->offset;
    cur_entry->tstamp = upd_entry->tstamp;
}

// Updates an entry from the entries hash, not from pending.
// Use update_regular_entry on pending hash entries instead.
// While iterating, regular entries will become entry lists,
// otherwise the result is a regular, single value entry.
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
            update_kd_entry_list(cur_entry, upd_entry, iterating);
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
            new_entry->epoch = upd_entry->epoch;
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

// Remove entry from either hash and free its memory.
static void remove_entry(bitcask_keydir* keydir, khiter_t itr)
{
    bitcask_keydir_entry * entry = kh_key(keydir->entries, itr);
    kh_del(entries, keydir->entries, itr);
    free_entry(entry);
}

static void perhaps_sweep_siblings(bitcask_keydir* keydir)
{
    int i;
    bitcask_keydir_entry* current_entry;
    bitcask_keydir_entry_proxy proxy;
    struct timeval target, now;
    suseconds_t max_usec = 600;

    assert(keydir != NULL);

    /* fprintf(stderr, "keydir iter_mutation %d sweep_last_generation %d iter_generation %d\r\n", keydir->iter_mutation,keydir->sweep_last_generation,keydir->iter_generation); */
    if (keydir->keyfolders > 0 ||
        keydir->iter_mutation == 0 ||
        keydir->sweep_last_generation == keydir->iter_generation)
    {
        return;
    }

#ifdef  PULSE
    i = 10;
#else   /* PULSE */
    i = 100*1000;
#endif

    gettimeofday(&target, NULL);
    target.tv_usec += max_usec;
    if (target.tv_usec > 1000000)
    {
        target.tv_sec++;
        target.tv_usec = target.tv_usec % 1000000;
    }
    while (i--)
    {
        if ((i % 500) == 0)
        {
            gettimeofday(&now, NULL);
            if (now.tv_sec > target.tv_usec &&
                now.tv_usec > target.tv_usec)
            {
                break;
            }
        }
        if (keydir->sweep_itr >= kh_end(keydir->entries))
        {
            keydir->sweep_itr = kh_begin(keydir->entries);
            keydir->sweep_last_generation = keydir->iter_generation;
            return;
        }
        if (kh_exist(keydir->entries, keydir->sweep_itr))
        {
            current_entry = kh_key(keydir->entries, keydir->sweep_itr);
            if (IS_ENTRY_LIST(current_entry))
            {
                if (proxy_kd_entry(current_entry, &proxy))
                {
                    if (proxy.is_tombstone)
                    {
                        remove_entry(keydir, keydir->sweep_itr);
                    }
                    else
                    {
                        update_entry(keydir, current_entry, &proxy);
                    }
                }
            }
        }
        keydir->sweep_itr++;
    }
}

// Adds a tombstone to an existing entries hash entry. Regular entries are
// converted to lists first. Only to be called during iterations.
// Entries are simply removed when there are no iterations.
static void set_entry_tombstone(bitcask_keydir* keydir, khiter_t itr,
                                uint32_t remove_time,
                                uint64_t remove_epoch)
{
    bitcask_keydir_entry_proxy tombstone;
    tombstone.tstamp = remove_time;
    tombstone.epoch = remove_epoch;
    tombstone.offset = MAX_OFFSET;
    tombstone.total_sz = MAX_SIZE;
    tombstone.file_id = MAX_FILE_ID;
    tombstone.key_sz = 0;

    bitcask_keydir_entry * entry= kh_key(keydir->entries, itr);
    if (!IS_ENTRY_LIST(entry))
    {
        // update into an entry list
        bitcask_keydir_entry* new_entry_list;
        new_entry_list = new_kd_entry_list(entry, &tombstone);
        kh_key(keydir->entries, itr) = new_entry_list;
        free(entry);
    }
    else
    {
        //need to update the entry list with a tombstone
        update_kd_entry_list(entry, &tombstone, keydir->keyfolders > 0);
    }
}

// Adds or updates an entry in the pending hash if they keydir is frozen
// or in the entries hash otherwise.
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
    // Not found and not frozen, add to entries
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
    uint32_t nowsec;
    uint32_t newest_put;
    uint32_t old_file_id;
    uint64_t old_offset;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint(env, argv[2], &(entry.file_id)) &&
        enif_get_uint(env, argv[3], &(entry.total_sz)) &&
        enif_get_uint64_bin(env, argv[4], &(entry.offset)) &&
        enif_get_uint(env, argv[5], &(entry.tstamp)) &&
        enif_get_uint(env, argv[6], &(nowsec)) &&
        enif_get_uint(env, argv[7], &(newest_put)) &&
        enif_get_uint(env, argv[8], &(old_file_id)) &&
        enif_get_uint64_bin(env, argv[9], &(old_offset)))
    {
        bitcask_keydir* keydir = handle->keydir;
        entry.key = (char*)key.data;
        entry.key_sz = key.size;

        LOCK(keydir);
        DEBUG2("LINE %d put\r\n", __LINE__);

        DEBUG_BIN(dbgKey, key.data, key.size);
        DEBUG("+++ Put key = %s file_id=%d offset=%d total_sz=%d tstamp=%u old_file_id=%d\r\n",
                dbgKey,
              (int) entry.file_id, (int) entry.offset,
              (int)entry.total_sz, (unsigned) entry.tstamp, (int)old_file_id);
        DEBUG_KEYDIR(keydir);

        perhaps_sweep_siblings(handle->keydir);

        find_result f;
        find_keydir_entry(keydir, &key, MAX_EPOCH, &f);

        // If conditional put and not found, bail early
        if ((!f.found || f.proxy.is_tombstone)
                && old_file_id != 0)
        {
            DEBUG2("LINE %d put -> already_exists\r\n", __LINE__);
            UNLOCK(keydir);
            return ATOM_ALREADY_EXISTS;
        }

        keydir->epoch += 1; //don't worry about backing this out if we bail
        entry.epoch = keydir->epoch;

        // If put would resize and iterating, start pending hash
        if (kh_put_will_resize(entries, keydir->entries) &&
            keydir->keyfolders != 0 &&
            (keydir->pending == NULL))
        {
            keydir->pending = kh_init(entries);
            keydir->pending_start_epoch = keydir->epoch;
            keydir->pending_start_time = nowsec;
        }

        if (!f.found || f.proxy.is_tombstone)
        {
            if ((newest_put &&
                 (entry.file_id < keydir->biggest_file_id)) ||
                old_file_id != 0) {
                /*
                 * Really, it doesn't exist.  But the atom 'already_exists'
                 * is also a signal that a merge has incremented the
                 * keydir->biggest_file_id and that we need to retry this
                 * operation after Erlang-land has re-written the key & val
                 * to a new location in the same-or-bigger file id.
                 */
                DEBUG2("LINE %d put -> already_exists\r\n", __LINE__);
                UNLOCK(keydir);
                return ATOM_ALREADY_EXISTS;
            }

            keydir->key_count++;
            keydir->key_bytes += key.size;
            if (keydir->keyfolders > 0)
            {
                keydir->iter_mutation = 1;
            }

            // Increment live and total stats.
            update_fstats(env, keydir, entry.file_id, entry.tstamp, MAX_EPOCH,
                          1, 1, entry.total_sz, entry.total_sz, 1);

            put_entry(keydir, &f, &entry);

            DEBUG("+++ Put new\r\n");
            DEBUG_KEYDIR(keydir);

            DEBUG2("LINE %d put -> ok (!found || !tombstone)\r\n", __LINE__);
            UNLOCK(keydir);
            return ATOM_OK;
        }

        // Putting only if replacing this file/offset entry, fail otherwise.
        // This is an important part of our optimistic concurrency mechanisms
        // to resolve races between writers (main and merge currently).
        if (old_file_id != 0 &&
                // This line is tricky: We are trying to detect a merge putting
                // a value that replaces another value that same merge just put
                // (so same output file).  Because when it does that, it has
                // replaced a previous value with smaller file/offset.  It then
                // found yet another value that is also current and should
                // be written to the merge file, but since it has smaller file/ofs
                // than the newly merged value (in a new merge file), it is
                // ignored. This happens with values from the same second,
                // since the out of date logic in merge uses timestamps.
            (newest_put || entry.file_id != f.proxy.file_id) &&
            !(old_file_id == f.proxy.file_id &&
              old_offset == f.proxy.offset))
        {
            DEBUG("++ Conditional not match\r\n");
            DEBUG2("LINE %d put -> already_exists/cond bad match\r\n", __LINE__);
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
            if (keydir->keyfolders > 0)
            {
                keydir->iter_mutation = 1;
            }
            // Remove the stats for the old entry and add the new
            if (f.proxy.file_id != entry.file_id) // different files
            {
                update_fstats(env, keydir, f.proxy.file_id, 0, MAX_EPOCH,
                              -1, 0,
                              -f.proxy.total_sz, 0, 0);
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              MAX_EPOCH, 1, 1,
                              entry.total_sz, entry.total_sz, 1);
            }
            else // file_id is same, change live/total in one entry
            {
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              MAX_EPOCH, 0, 1,
                              entry.total_sz - f.proxy.total_sz,
                              entry.total_sz, 1);
            }

            put_entry(keydir, &f, &entry);
            DEBUG2("LINE %d put -> ok\r\n", __LINE__);
            UNLOCK(keydir);
            DEBUG("Finished put\r\n");
            DEBUG_KEYDIR(keydir);
            return ATOM_OK;
        }
        else
        {
            // If not live yet, live stats are not updated, but total stats are
            if (!keydir->is_ready)
            {
                update_fstats(env, keydir, entry.file_id, entry.tstamp,
                              MAX_EPOCH, 0, 1, 0, entry.total_sz, 1);
            }
            DEBUG2("LINE %d put -> already_exists end\r\n", __LINE__);
            UNLOCK(keydir);
            DEBUG("No update\r\n");
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
    uint64 epoch; //intentionally odd type to get around warnings

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint64(env, argv[2], &epoch))
    {
        bitcask_keydir* keydir = handle->keydir;
        LOCK(keydir);

        DEBUG_BIN(dbgKey, key.data, key.size);
        DEBUG("+++ Get %s time = %lu\r\n", dbgKey, epoch);

        perhaps_sweep_siblings(handle->keydir);

        find_result f;
        find_keydir_entry(keydir, &key, epoch, &f);

        if (f.found && !f.proxy.is_tombstone)
        {
            ERL_NIF_TERM result;
            result = enif_make_tuple6(env,
                                      ATOM_BITCASK_ENTRY,
                                      argv[1], /* Key */
                                      enif_make_uint(env, f.proxy.file_id),
                                      enif_make_uint(env, f.proxy.total_sz),
                                      enif_make_uint64_bin(env, f.proxy.offset),
                                      enif_make_uint(env, f.proxy.tstamp));
            DEBUG(" ... returned value file id=%u size=%u ofs=%u tstamp=%u tomb=%u\r\n",
                  f.proxy.file_id, f.proxy.total_sz, f.proxy.offset, f.proxy.tstamp,
                  (unsigned)f.proxy.is_tombstone);
            DEBUG_ENTRY(f.entries_entry ? f.entries_entry : f.pending_entry);
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

ERL_NIF_TERM bitcask_nifs_keydir_get_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        LOCK(handle->keydir);
        uint64 epoch = handle->keydir->epoch;
        UNLOCK(handle->keydir);
        return enif_make_uint64(env, epoch);
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

        keydir->epoch += 1; // never back out, even if we don't mutate

        DEBUG("+++ Remove %s\r\n", is_conditional ? "conditional" : "");
        DEBUG_KEYDIR(keydir);

        perhaps_sweep_siblings(handle->keydir);

        find_result fr;
        find_keydir_entry(keydir, &key, keydir->epoch, &fr);

        if (fr.found && !fr.proxy.is_tombstone)
        {
            // If a conditional remove, bail if not a match.
            if (is_conditional &&
                (fr.proxy.tstamp != tstamp ||
                 fr.proxy.file_id != file_id ||
                 fr.proxy.offset != offset))
            {
                UNLOCK(keydir);
                DEBUG("+++Conditional no match\r\n");
                return ATOM_ALREADY_EXISTS;
            }

            // Remove the key from the keydir stats
            keydir->key_count--;
            keydir->key_bytes -= fr.proxy.key_sz;
            if (keydir->keyfolders > 0)
            {
                keydir->iter_mutation = 1;
            }

            // Remove from file stats
            update_fstats(env, keydir, fr.proxy.file_id, fr.proxy.tstamp,
                          MAX_EPOCH, -1, 0, -fr.proxy.total_sz, 0, 0);

            // If found an entry in the pending hash, convert it to a tombstone
            if (fr.pending_entry)
            {
                DEBUG2("LINE %d pending put\r\n", __LINE__);
                set_pending_tombstone(fr.pending_entry);
                fr.pending_entry->tstamp = remove_time;
                fr.pending_entry->epoch = keydir->epoch;
            }
            // If frozen, add tombstone to pending hash (iteration must have
            // started between put/remove call in bitcask:delete.
            else if (keydir->pending)
            {
                DEBUG2("LINE %d pending put\r\n", __LINE__);
                bitcask_keydir_entry* pending_entry =
                    add_entry(keydir, keydir->pending, &fr.proxy);
                set_pending_tombstone(pending_entry);
                pending_entry->tstamp = remove_time;
                pending_entry->epoch = keydir->epoch;
            }
            // If not iterating, just remove.
            else if(keydir->keyfolders == 0)
            { 
                remove_entry(keydir, fr.itr);
            }
            // else found in entries while iterating
            else
            {
                set_entry_tombstone(keydir, fr.itr, remove_time, keydir->epoch);
            }
            DEBUG("Removed\r\n");
            DEBUG_KEYDIR(keydir);

            UNLOCK(keydir);
            return ATOM_OK;;
        }
        else // not found
        {
            DEBUG("Not found - not removed\r\n");
            UNLOCK(keydir);
            return ATOM_OK;;
        }
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
            DEBUG2("LINE %d pending copy\r\n", __LINE__);
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
static int can_itr_keydir(bitcask_keydir* keydir, uint32_t ts, int maxage, int maxputs)
{
    if (keydir->pending == NULL ||   // not frozen or caller wants to reuse
        (maxage < 0 && maxputs < 0)) // the exiting freeze
    {
        DEBUG2("LINE %d can_itr\r\n", __LINE__);
        return 1;
    }
    else if (ts == 0 || ts < keydir->pending_start_time)
    {             // if clock skew (or forced wait), force key folding to wait
        DEBUG2("LINE %d can_itr\r\n", __LINE__);
        return 0; // which will fix keydir->pending_start
    }

    {
        DEBUG2("LINE %d can_itr\r\n", __LINE__);
        uint64_t age = ts - keydir->pending_start_time;
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
            keydir->epoch += 1;

            handle->iterating = 1;
            handle->epoch = keydir->epoch;
            keydir->newest_folder = keydir->epoch;
            keydir->keyfolders++;
            handle->iterator = kh_begin(keydir->entries);
            DEBUG2("LINE %d itr started, keydir->pending = 0x%lx\r\n", __LINE__, keydir->pending);
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
            DEBUG2("LINE %d itr\r\n", __LINE__);
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
        DEBUG("+++ itr next\r\n");
        bitcask_keydir* keydir = handle->keydir;

        if (handle->iterating != 1)
        {
            DEBUG("Itr not started\r\n");
            // Iteration not started!
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATION_NOT_STARTED);
        }

        LOCK(keydir);

        while (handle->iterator != kh_end(keydir->entries))
        {
            if (kh_exist(keydir->entries, handle->iterator))
            {
                DEBUG2("LINE %d itr_next\r\n", __LINE__);
                bitcask_keydir_entry* entry = kh_key(keydir->entries, handle->iterator);
                ErlNifBinary key;
                bitcask_keydir_entry_proxy proxy;

                if (!proxy_kd_entry_at_epoch(entry, handle->epoch, &proxy)
                    || proxy.is_tombstone)
                {
                    DEBUG("No value for itr_next");
                    // No value in the snapshot for the iteration time
                    (handle->iterator)++;
                    continue;
                }
                DEBUG_BIN(dbgKey, proxy.key, proxy.key_sz);
                DEBUG("itr_next key=%s", dbgKey);

                // Alloc the binary and make sure it succeeded
                if (!enif_alloc_binary_compat(env, proxy.key_sz, &key))
                {
                    UNLOCK(keydir);
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
                UNLOCK(keydir);
                DEBUG("Found entry\r\n");
                DEBUG_ENTRY(entry);
                return curr;
            }
            else
            {
                // No item in this slot; increment the iterator and keep looping
                (handle->iterator)++;
            }
        }

        UNLOCK(keydir);
        // The iterator is at the end of the table
        return ATOM_NOT_FOUND;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

void itr_release_internal(ErlNifEnv* env, bitcask_keydir_handle* handle)
{
    handle->iterating = 0;
    handle->keydir->keyfolders--;
    handle->epoch = MAX_EPOCH;

    // If last iterator closing, unfreeze keydir and merge pending entries.
    if (handle->keydir->keyfolders == 0 && handle->keydir->pending != NULL)
    {
        DEBUG2("LINE %d itr_release\r\n", __LINE__);
        merge_pending_entries(env, handle->keydir);
        handle->keydir->iter_generation++;
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

        itr_release_internal(env, handle);

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
        //                                   oldest_tstamp, newest_tstamp,
        //                                   expiration_epoch}]
        ERL_NIF_TERM fstats_list = enif_make_list(env, 0);
        khiter_t itr;
        bitcask_fstats_entry* curr_f;
        for (itr = kh_begin(keydir->fstats); itr != kh_end(keydir->fstats); ++itr)
        {
            if (kh_exist(keydir->fstats, itr))
            {
                curr_f = kh_val(keydir->fstats, itr);
                ERL_NIF_TERM fstat =
                    enif_make_tuple8(env,
                                     enif_make_uint(env, curr_f->file_id),
                                     enif_make_ulong(env, curr_f->live_keys),
                                     enif_make_ulong(env, curr_f->total_keys),
                                     enif_make_ulong(env, curr_f->live_bytes),
                                     enif_make_ulong(env, curr_f->total_bytes),
                                     enif_make_uint(env, curr_f->oldest_tstamp),
                                     enif_make_uint(env, curr_f->newest_tstamp),
                                     enif_make_uint64(env, (ErlNifUInt64)curr_f->expiration_epoch));
                fstats_list = enif_make_list_cell(env, fstat, fstats_list);
            }
        }

        ERL_NIF_TERM iter_info =
            enif_make_tuple4(env,
                             enif_make_uint64(env, keydir->iter_generation),
                             enif_make_ulong(env, keydir->keyfolders),
                             keydir->pending == NULL ? ATOM_FALSE : ATOM_TRUE,
                             keydir->pending == NULL ? ATOM_UNDEFINED :
                             enif_make_uint64(env, keydir->pending_start_epoch));

        ERL_NIF_TERM result = enif_make_tuple5(env,
                                               enif_make_uint64(env, keydir->key_count),
                                               enif_make_uint64(env, keydir->key_bytes),
                                               fstats_list,
                                               iter_info,
                                               enif_make_uint64(env, keydir->epoch));
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

ERL_NIF_TERM bitcask_nifs_increment_file_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    uint32_t conditional_file_id = 0;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {

        if (argc == 2)
        {
            enif_get_uint(env, argv[1], &(conditional_file_id));
        }
        LOCK(handle->keydir);
        if (conditional_file_id == 0)
        {
            (handle->keydir->biggest_file_id)++;
        }
        else
        {
            if (conditional_file_id > handle->keydir->biggest_file_id)
            {
                handle->keydir->biggest_file_id = conditional_file_id;
            }
        }
        uint32_t id = handle->keydir->biggest_file_id;
        UNLOCK(handle->keydir);
        return enif_make_tuple2(env, ATOM_OK, enif_make_uint(env, id));
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_trim_fstats(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    ERL_NIF_TERM head, tail, list;
    uint32_t non_existent_entries = 0;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle)&&
        enif_is_list(env, argv[1]))
    {
        bitcask_keydir* keydir = handle->keydir;
        
        LOCK(keydir);
        uint32_t file_id;

        list = argv[1];

        while (enif_get_list_cell(env, list, &head, &tail))
        {
            enif_get_uint(env, head, &file_id);

            khiter_t itr = kh_get(fstats, keydir->fstats, file_id);
            if (itr != kh_end(keydir->fstats))
            {
                bitcask_fstats_entry* curr_f;
                curr_f = kh_val(keydir->fstats, itr);
                free(curr_f);
                kh_del(fstats, keydir->fstats, itr);
            }
            else
            {
                non_existent_entries++;
            }
            // if not found, noop, but shouldn't happen.
            // think about chaning the retval to signal for warning?
            list = tail;
        }
        UNLOCK(keydir);
        return enif_make_tuple2(env, ATOM_OK, 
                                enif_make_uint(env, non_existent_entries));
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
    int flags = O_RDWR | O_APPEND;
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

// Returns 0 if failed to parse lseek style argument for file_position
static int parse_seek_offset(ErlNifEnv* env, ERL_NIF_TERM arg, off_t * ofs, int * whence)
{
    long long_ofs;
    int arity;
    const ERL_NIF_TERM* tuple_elements;
    if (enif_get_long(env, arg, &long_ofs))
    {
        *whence = SEEK_SET;
        *ofs = (off_t)long_ofs;
        return 1;
    }
    else if (enif_get_tuple(env, arg, &arity, &tuple_elements) && arity == 2
            && enif_get_long(env, tuple_elements[1], &long_ofs))
    {
        *ofs = (off_t)long_ofs;
        if (tuple_elements[0] == ATOM_CUR)
        {
            *whence = SEEK_CUR;
        }
        else if (tuple_elements[0] == ATOM_BOF)
        {
            *whence = SEEK_SET;
        }
        else if (tuple_elements[0] == ATOM_EOF)
        {
            *whence = SEEK_END;
        }
        else
        {
            return 0;
        }
        return 1;
    }
    else
    {
        return 0;
    }
}

ERL_NIF_TERM bitcask_nifs_file_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;
    off_t offset;
    int whence;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle) &&
        parse_seek_offset(env, argv[1], &offset, &whence))
    {

        off_t new_offset = lseek(handle->fd, offset, whence);
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
        if (lseek(handle->fd, 0, SEEK_SET) != (off_t)-1)
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

ERL_NIF_TERM bitcask_nifs_file_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_file_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_file_RESOURCE, (void**)&handle))
    {
        off_t ofs = lseek(handle->fd, 0, SEEK_CUR);
        if (ofs == (off_t)-1)
        {
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }

        if (ftruncate(handle->fd, ofs) == -1)
        {
            return errno_error_tuple(env, ATOM_FTRUNCATE_ERROR, errno);
        }

        return ATOM_OK;
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
    DEBUG("Merge pending entries. Keydir before merging\r\n");
    DEBUG_KEYDIR(keydir);

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
    DEBUG2("LINE %d keydir->pending = NULL\r\n", __LINE__);
    keydir->pending = NULL;

    keydir->pending_updated = 0;
    keydir->pending_start_time = 0;
    keydir->pending_start_epoch = 0;
    if (keydir->pending_awaken != NULL)
    {
        free(keydir->pending_awaken);
    }
    keydir->pending_awaken = NULL;
    keydir->pending_awaken_count = 0;
    keydir->pending_awaken_size = 0;

    DEBUG("Merge pending entries completed\r\n");
    DEBUG_KEYDIR(keydir);
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
    free(keydir);
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
        if (handle->iterating)
        {
            LOCK(handle->keydir);

            itr_release_internal(env, handle);

            UNLOCK(handle->keydir);
        }

        handle->keydir = 0;
    }

    // If the keydir has a lock, we need to decrement the refcount and
    // potentially release it
    if (keydir->mutex)
    {
        bitcask_priv_data* priv = (bitcask_priv_data*)enif_priv_data(env);
        enif_mutex_lock(priv->global_keydirs_lock);

        // Remember biggest_file_id in case someone re-opens the same name
        uint32_t global_biggest = 0, the_biggest = 0;
        khiter_t itr_biggest_file_id = kh_get(global_biggest_file_id, priv->global_biggest_file_id, keydir->name);
        if (itr_biggest_file_id != kh_end(priv->global_biggest_file_id)) {
            global_biggest = kh_val(priv->global_biggest_file_id, itr_biggest_file_id);
        }
        the_biggest = (global_biggest > keydir->biggest_file_id) ? \
            global_biggest : keydir->biggest_file_id;
        the_biggest++;
        kh_put2(global_biggest_file_id, priv->global_biggest_file_id, strdup(keydir->name), the_biggest);

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
void dump_fstats(bitcask_keydir* keydir)
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
    priv->global_biggest_file_id = kh_init(global_biggest_file_id);
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
    ATOM_UNDEFINED = enif_make_atom(env, "undefined");
    ATOM_EOF = enif_make_atom(env, "eof");
    ATOM_CREATE = enif_make_atom(env, "create");
    ATOM_READONLY = enif_make_atom(env, "readonly");
    ATOM_O_SYNC = enif_make_atom(env, "o_sync");
    ATOM_CUR = enif_make_atom(env, "cur");
    ATOM_BOF = enif_make_atom(env, "bof");

#ifdef PULSE
    pulse_c_send_on_load(env);
#endif

    return 0;
}

ERL_NIF_INIT(bitcask_nifs, nif_funcs, &on_load, NULL, NULL, NULL);
