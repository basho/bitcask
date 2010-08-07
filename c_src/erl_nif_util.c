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

#include "erl_nif_util.h"
#include "erl_nif_compat.h"

#include <string.h>

int enif_get_uint64_bin(ErlNifEnv* env, ERL_NIF_TERM term, uint64_t* outvalue)
{
    ErlNifBinary bin;
    if (enif_inspect_binary(env, term, &bin) && bin.size == sizeof(uint64_t))
    {
        memcpy(outvalue, ((uint64_t*)bin.data), sizeof(uint64_t));
        return 1;
    }
    else
    {
        return 0;
    }
}

ERL_NIF_TERM enif_make_uint64_bin(ErlNifEnv* env, uint64_t value)
{
    ErlNifBinary bin;
    enif_alloc_binary_compat(env, sizeof(uint64_t), &bin);
    memcpy(bin.data, &value, sizeof(uint64_t));
    return enif_make_binary(env, &bin);
}

