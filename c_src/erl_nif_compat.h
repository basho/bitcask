/* Copyright (c) 2010-2011 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/

#ifndef ERL_NIF_COMPAT_H_
#define ERL_NIF_COMPAT_H_

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include "erl_nif.h"

#if ERL_NIF_MAJOR_VERSION == 1 && ERL_NIF_MINOR_VERSION == 0

#define enif_open_resource_type_compat enif_open_resource_type
#define enif_alloc_resource_compat enif_alloc_resource
#define enif_release_resource_compat enif_release_resource
#define enif_alloc_binary_compat enif_alloc_binary
#define enif_alloc_compat enif_alloc
#define enif_realloc_compat enif_realloc
#define enif_free_compat enif_free
#define enif_cond_create erl_drv_cond_create
#define enif_cond_destroy erl_drv_cond_destroy
#define enif_cond_signal erl_drv_cond_signal
#define enif_cond_broadcast erl_drv_cond_broadcast
#define enif_cond_wait erl_drv_cond_wait
#define ErlNifCond ErlDrvCond
#endif /* R13B04 */

#if ERL_NIF_MAJOR_VERSION == 2 && ERL_NIF_MINOR_VERSION >= 0

#define enif_open_resource_type_compat(E, N, D, F, T) \
    enif_open_resource_type(E, NULL, N, D, F, T)

#define enif_alloc_resource_compat(E, T, S) \
    enif_alloc_resource(T, S)

#define enif_release_resource_compat(E, H) \
    enif_release_resource(H)

#define enif_alloc_binary_compat(E, S, B) \
    enif_alloc_binary(S, B)

#define enif_alloc_compat(E, S) \
    enif_alloc(S)

#define enif_realloc_compat(E, P, S) \
    enif_realloc(P, S)

#define enif_free_compat(E, P) \
    enif_free(P)

#endif /* R14 */



#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* ERL_NIF_COMPAT_H_ */
