/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:   log_header.h
* Author: 
* Date:         2016/07/27
* Version:      1.0
* Description:
* 
************************************************/
#ifndef LOG_HEADER_H_
#define LOG_HEADER_H_
#include <cstdint>

typedef enum {
    CRC_32,
    CRC_64,
    MD5_128
}checksum_type_t;// define checksum type
// define checksum payload data length
#define CRC_32_LEN      4
#define CRC_64_LEN      8
#define MD5_128_LEN     16
typedef union {
    uint32_t crc_32;
    uint64_t crc_64;
    uint8_t  md5_128[MD5_128_LEN];
}checksum_t;

typedef enum {
    LOG_IO,
    SNAPSHOT_CREATE,
    SNAPSHOT_DELETE,
    SNAPSHOT_ROLLBACK,
}log_type_t;

#pragma pack(1)
typedef struct journal_header {
    int32_t         version;
    checksum_type_t checksum_type;
}journal_header_t;  // exists at the start of every journal file

typedef struct off_len {
    uint64_t offset;
    uint32_t length;
}off_len_t;

typedef struct log_header {
    log_type_t type;         // io or snapshot
    checksum_t checksum;     // this is a union,use the right member according to the checksum_type in journal_header
    uint8_t    count;        // merged io count, if type==SNAPSHOT, set count=0
    off_len_t  off_len[0];   // io offset and length; the actual header legth should be sizeof(log_header_t) + .count * sizeof(off_len_t)
}log_header_t;
#pragma pack()

#endif
