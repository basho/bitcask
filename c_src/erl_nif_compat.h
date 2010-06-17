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
#define enif_free_compat enif_free
#endif /* R13B04 */

#if ERL_NIF_MAJOR_VERSION == 2 && ERL_NIF_MINOR_VERSION == 0

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

#define enif_free_compat(E, P) \
    enif_free(P)

#endif /* R14 */



#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* ERL_NIF_COMPAT_H_ */
