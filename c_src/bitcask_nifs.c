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

#include "erl_nif.h"
#include "uthash.h"

#include <fcntl.h>
#include <unistd.h>

static ErlNifResourceType* bitcask_keydir_RESOURCE;

typedef struct
{
    UT_hash_handle hh;         /* Required for uthash */
    uint32_t file_id;
    uint32_t value_sz;
    uint64_t value_pos;
    uint32_t tstamp;
    uint16_t key_sz;
    char     key[0];
} bitcask_keydir_entry;

typedef struct
{
    UT_hash_handle hh;         /* Required for uthash */
    bitcask_keydir_entry* entries;
    bitcask_keydir_entry* iterator;
    size_t        key_count;
    size_t        key_bytes;
    unsigned int  refcount;
    ErlNifRWLock* lock;
    char          name[0];
} bitcask_keydir;

typedef struct
{
    bitcask_keydir* keydir;
} bitcask_keydir_handle;

typedef struct
{
    bitcask_keydir* global_keydirs;
    ErlNifMutex*    global_keydirs_lock;
} bitcask_priv_data;

// Keydir hashtable functions
#define KEYDIR_HASH_FIND(head, bin, out) HASH_FIND(hh, head, bin.data, bin.size, out)
#define KEYDIR_HASH_ADD(head, entry) HASH_ADD(hh, head, key, entry->key_sz, entry)

// Global keydirs hashtable functions
#define GKEYDIR_HASH_FIND(head, name, out) HASH_FIND(hh, head, name, strlen(name), out)
#define GKEYDIR_HASH_ADD(head, kd) HASH_ADD(hh, head, name, strlen(kd->name), kd)

// Handle lock helper functions
#define R_LOCK(handle)    { if (keydir->lock) enif_rwlock_rlock(keydir->lock); }
#define R_UNLOCK(handle)  { if (keydir->lock) enif_rwlock_runlock(keydir->lock); }
#define RW_LOCK(handle)   { if (keydir->lock) enif_rwlock_rwlock(keydir->lock); }
#define RW_UNLOCK(handle) { if (keydir->lock) enif_rwlock_rwunlock(keydir->lock); }

// Prototypes
ERL_NIF_TERM bitcask_nifs_keydir_new0(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_new1(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_copy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM bitcask_nifs_create_file(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"keydir_new", 0, bitcask_nifs_keydir_new0},
    {"keydir_new", 1, bitcask_nifs_keydir_new1},
    {"keydir_put", 6, bitcask_nifs_keydir_put},
    {"keydir_get", 2, bitcask_nifs_keydir_get},
    {"keydir_remove", 2, bitcask_nifs_keydir_remove},
    {"keydir_copy", 1, bitcask_nifs_keydir_copy},
    {"keydir_itr", 1, bitcask_nifs_keydir_itr},
    {"keydir_itr_next", 1, bitcask_nifs_keydir_itr_next},
    {"keydir_info", 1, bitcask_nifs_keydir_info},

    {"create_file", 1, bitcask_nifs_create_file}
};

ERL_NIF_TERM bitcask_nifs_keydir_new0(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // First, setup a resource for our handle
    bitcask_keydir_handle* handle = enif_alloc_resource(env,
                                                        bitcask_keydir_RESOURCE,
                                                        sizeof(bitcask_keydir_handle));

    // Now allocate the actual keydir instance. Because it's unnamed/shared, we'll
    // leave the name and lock portions null'd out
    bitcask_keydir* keydir = enif_alloc(env, sizeof(bitcask_keydir));
    memset(keydir, '\0', sizeof(bitcask_keydir));

    // Assign the keydir to our handle and hand it back
    handle->keydir = keydir;
    ERL_NIF_TERM result = enif_make_resource(env, handle);
    enif_release_resource(env, handle);
    return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
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
        GKEYDIR_HASH_FIND(priv->global_keydirs, name, keydir);
        if (keydir)
        {
            // Existing keydir is available. Increment the refcount.
            keydir->refcount++;
        }
        else
        {
            // No such keydir, create a new one and add to the globals list. Make sure
            // to allocate enough room for the name.
            keydir = enif_alloc(env, sizeof(bitcask_keydir) + name_sz + 1);
            memset(keydir, '\0', sizeof(bitcask_keydir) + name_sz + 1);
            strncpy(keydir->name, name, name_sz);

            // Be sure to initialize the rwlock
            keydir->lock = enif_rwlock_create(name);

            // Finally, register this new keydir in the globals
            GKEYDIR_HASH_ADD(priv->global_keydirs, keydir);
        }

        enif_mutex_unlock(priv->global_keydirs_lock);

        // Setup a resource for the handle
        bitcask_keydir_handle* handle = enif_alloc_resource(env,
                                                            bitcask_keydir_RESOURCE,
                                                            sizeof(bitcask_keydir_handle));
        handle->keydir = keydir;
        ERL_NIF_TERM result = enif_make_resource(env, handle);
        enif_release_resource(env, handle);
        return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    bitcask_keydir_entry entry;
    ErlNifBinary key;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_get_uint(env, argv[2], (unsigned int*)&(entry.file_id)) &&
        enif_get_uint(env, argv[3], &(entry.value_sz)) &&
        enif_get_ulong(env, argv[4], (unsigned long*)&(entry.value_pos)) &&
        enif_get_uint(env, argv[5], &(entry.tstamp)))
    {
        bitcask_keydir* keydir = handle->keydir;
        RW_LOCK(keydir);

        // Now that we've marshalled everything, see if the tstamp for this key is >=
        // to what's already in the hash. Otherwise, we don't bother with the update.
        bitcask_keydir_entry* old_entry = 0;
        KEYDIR_HASH_FIND(keydir->entries, key, old_entry);
        if (old_entry == 0)
        {
            // No entry exists at all yet; add one
            bitcask_keydir_entry* new_entry = enif_alloc(env, sizeof(bitcask_keydir_entry) +
                                                         key.size);
            new_entry->file_id = entry.file_id;
            new_entry->value_sz = entry.value_sz;
            new_entry->value_pos = entry.value_pos;
            new_entry->tstamp = entry.tstamp;
            new_entry->key_sz = key.size;
            memcpy(new_entry->key, key.data, key.size);
            KEYDIR_HASH_ADD(keydir->entries, new_entry);

            // Update the stats
            keydir->key_count++;
            keydir->key_bytes += key.size;

            // Reset the iterator to ensure that someone doesn't cause a crash
            // by trying to interleave change operations with iterations
            keydir->iterator = keydir->entries;

            RW_UNLOCK(keydir);
            return enif_make_atom(env, "ok");
        }
        else if (old_entry->tstamp <= entry.tstamp)
        {
            // Entry already exists -- let's just update the relevant info. Note that if you
            // do multiple updates in a second, the last one in wins!
            // TODO: Safe?
            old_entry->file_id = entry.file_id;
            old_entry->value_sz = entry.value_sz;
            old_entry->value_pos = entry.value_pos;
            old_entry->tstamp = entry.tstamp;
            RW_UNLOCK(keydir);
            return enif_make_atom(env, "ok");
        }
        else
        {
            RW_UNLOCK(keydir);
            return enif_make_atom(env, "already_exists");
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    ErlNifBinary key;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key))
    {
        bitcask_keydir* keydir = handle->keydir;
        R_LOCK(keydir);

        bitcask_keydir_entry* entry = 0;
        KEYDIR_HASH_FIND(keydir->entries, key, entry);
        if (entry != 0)
        {
            ERL_NIF_TERM result = enif_make_tuple6(env,
                                                   enif_make_atom(env, "bitcask_entry"),
                                                   argv[1], /* Key */
                                                   enif_make_uint(env, entry->file_id),
                                                   enif_make_uint(env, entry->value_sz),
                                                   enif_make_ulong(env, entry->value_pos),
                                                   enif_make_uint(env, entry->tstamp));
            R_UNLOCK(keydir);
            return result;
        }
        else
        {
            R_UNLOCK(keydir);
            return enif_make_atom(env, "not_found");
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
        bitcask_keydir* keydir = handle->keydir;
        RW_LOCK(keydir);

        bitcask_keydir_entry* entry = 0;
        KEYDIR_HASH_FIND(keydir->entries, key, entry);
        if (entry != 0)
        {
            // Update the stats
            keydir->key_count--;
            keydir->key_bytes -= entry->key_sz;

            HASH_DEL(keydir->entries, entry);

            // Reset the iterator to ensure that someone doesn't cause a crash
            // by trying to interleave change operations with iterations
            keydir->iterator = keydir->entries;

            enif_free(env, entry);
        }

        RW_UNLOCK(keydir);
        return enif_make_atom(env, "ok");
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
        R_LOCK(keydir);

        bitcask_keydir_handle* new_handle = enif_alloc_resource(env,
                                                                bitcask_keydir_RESOURCE,
                                                                sizeof(bitcask_keydir_handle));

        // Now allocate the actual keydir instance. Because it's unnamed/shared, we'll
        // leave the name and lock portions null'd out
        bitcask_keydir* new_keydir = enif_alloc(env, sizeof(bitcask_keydir));
        new_handle->keydir = new_keydir;
        memset(new_keydir, '\0', sizeof(bitcask_keydir));

        // Deep copy each item from the existing handle
        bitcask_keydir_entry* curr;
        bitcask_keydir_entry* new;
        for(curr = keydir->entries; curr != NULL; curr = curr->hh.next)
        {
            // Allocate our entry to be inserted into the new table and copy the record
            // over. Note that we skip the hh portion of the struct.
            size_t new_sz = sizeof(bitcask_keydir_entry) + curr->key_sz;
            new = enif_alloc(env, new_sz);
            memcpy(new, curr, new_sz);
            memset(new, '\0', sizeof(UT_hash_handle));
            KEYDIR_HASH_ADD(new_keydir->entries, new);
        }

        R_UNLOCK(keydir);

        ERL_NIF_TERM result = enif_make_resource(env, new_handle);
        enif_release_resource(env, new_handle);
        return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle))
    {
        bitcask_keydir* keydir = handle->keydir;

        // If this is a named keydir, we do not permit iteration for locking reasons.
        if (keydir->lock)
        {
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "iteration_not_permitted"));
        }

        // Initialize the iterator
        keydir->iterator = keydir->entries;
        return enif_make_atom(env, "ok");
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

        // If this is a named keydir, we do not permit iteration for locking reasons.
        if (keydir->lock)
        {
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_atom(env, "iteration_not_permitted"));
        }

        if (keydir->iterator)
        {
            bitcask_keydir_entry* entry = keydir->iterator;
            ErlNifBinary key;

            // Alloc the binary and make sure it succeeded
            if (!enif_alloc_binary(env, entry->key_sz, &key))
            {
                return enif_make_atom(env, "allocation_error");
            }

            // Copy the data from our key to the new allocated binary
            // TODO: If we maintained a ErlNifBinary in the original entry, could we
            // get away with not doing a copy here?
            memcpy(key.data, entry->key, entry->key_sz);
            ERL_NIF_TERM curr = enif_make_tuple6(env,
                                                 enif_make_atom(env, "bitcask_entry"),
                                                 enif_make_binary(env, &key),
                                                 enif_make_uint(env, entry->file_id),
                                                 enif_make_uint(env, entry->value_sz),
                                                 enif_make_ulong(env, entry->value_pos),
                                                 enif_make_uint(env, entry->tstamp));

            // Update the iterator to the next entry
            keydir->iterator = entry->hh.next;

            return curr;
        }
        else
        {
            return enif_make_atom(env, "not_found");
        }
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
        R_LOCK(keydir);
        ERL_NIF_TERM result = enif_make_tuple2(env,
                                               enif_make_ulong(env, keydir->key_count),
                                               enif_make_ulong(env, keydir->key_bytes));
        R_UNLOCK(keydir);
        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}


ERL_NIF_TERM bitcask_nifs_create_file(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char filename[4096];
    if (enif_get_string(env, argv[0], filename, sizeof(filename), ERL_NIF_LATIN1) > 0)
    {
        // Try to open the provided filename exclusively.
        int fd = open(filename, O_CREAT | O_EXCL, S_IREAD | S_IWRITE);
        if (fd > -1)
        {
            close(fd);
            return enif_make_atom(env, "true");
        }
        else
        {
            return enif_make_atom(env, "false");
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

static void free_keydir(ErlNifEnv* env, bitcask_keydir* keydir)
{
    // Delete all the entries in the hash table, which also has the effect of
    // freeing up all resources associated with the table.
    bitcask_keydir_entry* current_entry;
    while (keydir->entries)
    {
        current_entry = keydir->entries;
        HASH_DEL(keydir->entries, current_entry);
        enif_free(env, current_entry);
    }
}

static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg)
{
    bitcask_keydir_handle* handle = (bitcask_keydir_handle*)arg;
    bitcask_keydir* keydir = handle->keydir;

    // If the keydir has a lock, we need to decrement the refcount and
    // potentially release it
    if (keydir->lock)
    {
        bitcask_priv_data* priv = (bitcask_priv_data*)enif_priv_data(env);
        enif_mutex_lock(priv->global_keydirs_lock);
        keydir->refcount--;
        if (keydir->refcount == 0)
        {
            // This is the last reference to the named keydir. As such,
            // remove it from the hashtable so no one else tries to use it
            HASH_DEL(priv->global_keydirs, keydir);
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
        if (keydir->lock)
        {
            enif_rwlock_destroy(keydir->lock);
        }

        free_keydir(env, keydir);
    }
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    bitcask_keydir_RESOURCE = enif_open_resource_type(env, "bitcask_keydir_resource",
                                                      &bitcask_nifs_keydir_resource_cleanup,
                                                      ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                      0);
    // Initialize shared keydir hashtable
    bitcask_priv_data* priv = enif_alloc(env, sizeof(bitcask_priv_data));
    priv->global_keydirs = 0;
    priv->global_keydirs_lock = enif_mutex_create("bitcask_global_handles_lock");
    *priv_data = priv;

    return 0;
}

ERL_NIF_INIT(bitcask_nifs, nif_funcs, &on_load, NULL, NULL, NULL);
