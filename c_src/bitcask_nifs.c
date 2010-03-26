// -------------------------------------------------------------------
//
// bitcask: Eric Brewer-inspired key/value store
//
// Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
//
// innostore is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
//
// innostore is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with innostore.  If not, see <http://www.gnu.org/licenses/>.
//
// -------------------------------------------------------------------

#include "erl_nif.h"
#include "uthash.h"

static ErlNifResourceType* bitcask_keydir_RESOURCE;

typedef struct
{
    UT_hash_handle hh;         /* Required for uthash */
    uint16_t file_id;
    uint32_t value_sz;
    uint64_t value_pos;
    uint32_t tstamp;
    uint16_t key_sz;
    char     key[0];
} bitcask_keydir_entry;


typedef struct
{
    bitcask_keydir_entry* keydir;
} bitcask_keydir_handle;

// Hash table helper functions
#define KEYDIR_HASH_FIND(head, bin, out) HASH_FIND(hh, head, bin.data, bin.size, out)
#define KEYDIR_HASH_ADD(head, entry) HASH_ADD(hh, head, key, entry->key_sz, entry)


// Prototypes
ERL_NIF_TERM bitcask_nifs_keydir_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"keydir_new", 0, bitcask_nifs_keydir_new},
    {"keydir_put", 6, bitcask_nifs_keydir_put},
    {"keydir_get", 2, bitcask_nifs_keydir_get},
    {"keydir_remove", 2, bitcask_nifs_keydir_remove},
    {"keydir_itr", 1, bitcask_nifs_keydir_itr},
    {"keydir_itr_next", 1, bitcask_nifs_keydir_itr_next}
};

ERL_NIF_TERM bitcask_nifs_keydir_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle = enif_alloc_resource(env,
                                                       bitcask_keydir_RESOURCE,
                                                       sizeof(bitcask_keydir_handle));
    handle->keydir = 0;
    ERL_NIF_TERM result = enif_make_resource(env, handle);
    enif_release_resource(env, handle);
    return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
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
        // Now that we've marshalled everything, see if the tstamp for this key is >=
        // to what's already in the hash. Otherwise, we don't bother with the update.
        bitcask_keydir_entry* old_entry = 0;
        KEYDIR_HASH_FIND(handle->keydir, key, old_entry);
        if (old_entry == 0)
        {
            // No entry exists at all yet; add one
            bitcask_keydir_entry* new_entry = enif_alloc(env, sizeof(bitcask_keydir_entry) + key.size);
            new_entry->file_id = entry.file_id;
            new_entry->value_sz = entry.value_sz;
            new_entry->value_pos = entry.value_pos;
            new_entry->tstamp = entry.tstamp;
            new_entry->key_sz = key.size;
            memcpy(new_entry->key, key.data, key.size);
            KEYDIR_HASH_ADD(handle->keydir, new_entry);
            return enif_make_atom(env, "ok");
        }
        else if (old_entry->tstamp < entry.tstamp)
        {
            // Entry already exists -- let's just update the relevant info
            old_entry->file_id = entry.file_id;
            old_entry->value_sz = entry.value_sz;
            old_entry->value_pos = entry.value_pos;
            old_entry->tstamp = entry.tstamp;
            return enif_make_atom(env, "ok");
        }
        else
        {
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
        bitcask_keydir_entry* entry = 0;
        KEYDIR_HASH_FIND(handle->keydir, key, entry);
        if (entry != 0)
        {
            return enif_make_tuple6(env,
                                    enif_make_atom(env, "bitcask_entry"),
                                    argv[1], /* Key */
                                    enif_make_uint(env, entry->file_id),
                                    enif_make_uint(env, entry->value_sz),
                                    enif_make_ulong(env, entry->value_pos),
                                    enif_make_uint(env, entry->tstamp));
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


ERL_NIF_TERM bitcask_nifs_keydir_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_handle* handle;
    ErlNifBinary key;

    if (enif_get_resource(env, argv[0], bitcask_keydir_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key))
    {
        bitcask_keydir_entry* entry = 0;
        KEYDIR_HASH_FIND(handle->keydir, key, entry);
        if (entry != 0)
        {
            HASH_DEL(handle->keydir, entry);
            enif_free(env, entry);
        }

        return enif_make_atom(env, "ok");
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
        if (handle->keydir)
        {
            // Iteration of the keydir is uni-directional and immutable. As such, we are going
            // to avoid allocating another resource and just hand back a cast'd pointer as our
            // iterator. This means that iteration is very fast, but also somewhat fragile.
            return enif_make_ulong(env, (unsigned long)handle->keydir);
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

ERL_NIF_TERM bitcask_nifs_keydir_itr_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    bitcask_keydir_entry* entry = 0;

    // Remember, for iteration we just cast the entry pointer and deref it.
    if (enif_get_ulong(env, argv[0], (unsigned long*)(&entry)))
    {
        if (entry)
        {
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

            // Look at the next entry in the hashtable. If the next one is empty, we'll return
            // a zero and the next call to itr_next will then just return the not_found atom.
            ERL_NIF_TERM next = enif_make_ulong(env, (unsigned long)entry->hh.next);
            return enif_make_tuple2(env, curr, next);
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


static void bitcask_nifs_keydir_resource_cleanup(ErlNifEnv* env, void* arg)
{
    bitcask_keydir_handle* handle = (bitcask_keydir_handle*)arg;

    // Delete all the entries in the hash table, which also has the effect of
    // freeing up all resources associated with the table.
    bitcask_keydir_entry* current_entry;
    while (handle->keydir)
    {
        current_entry = handle->keydir;
        HASH_DEL(handle->keydir, current_entry);
        enif_free(env, current_entry);
    }
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    bitcask_keydir_RESOURCE = enif_open_resource_type(env, "bitcask_keydir_resource",
                                                      &bitcask_nifs_keydir_resource_cleanup,
                                                      ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                      0);
    return 0;
}

ERL_NIF_INIT(bitcask_nifs, nif_funcs, &on_load, NULL, NULL, NULL);
