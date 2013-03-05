//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the
// public domain. The author hereby disclaims copyright to this source
// code.

#ifndef _MURMURHASH3_H_
#define _MURMURHASH3_H_

#include <stdint.h>

//-----------------------------------------------------------------------------

      
void MurmurHash3_x86_32(const void *key, int len, uint32_t seed, void *out);

void MurmurHash3_x86_128(const void *key, int len, uint32_t seed, void *out);

void MurmurHash3_x64_128(const void *key, int len, uint32_t seed, void *out);


#if defined(__x86_64__)
uint64_t MurmurHash3_x86_64_64bit(const void * key, int len, unsigned int seed);
#define MURMUR_HASH MurmurHash3_x86_64_64bit

#elif defined(__i386__)
unsigned int MurmurHash3_x86_32bit(const void * key, int len, unsigned int seed);
#define MURMUR_HASH MurmurHash3_x86_32bit

#else
#error unsupported platform
#endif

//-----------------------------------------------------------------------------

#endif // _MURMURHASH3_H_
