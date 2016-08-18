#ifndef CRC32_HEADER_H
#define CRC32_HEADER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>

#if defined (__cplusplus)
extern "C" {
#endif

uint32_t crc32c(const void *buf, size_t len, uint32_t crc);

#if defined (__cplusplus)
}
#endif
#endif

