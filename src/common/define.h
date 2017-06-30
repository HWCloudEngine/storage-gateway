#ifndef DEFINE_H
#define DEFINE_H
#include <stdint.h>

/*****************journal entry**********************/
/*crc type*/
typedef enum 
{
    CRC_32,
    CRC_64,
    MD5_128
}checksum_type_t;

#pragma pack(1)
/*each journal file contain a header*/
struct journal_file_header
{
    uint8_t  magic;
    uint32_t version;
    uint32_t reserve;
};
typedef struct journal_file_header journal_file_header_t;

/*journal evnet type*/
enum journal_event_type
{
    /*comon write io*/
    IO_WRITE = 0,
    
    /*snapshot*/
    SNAPSHOT_CREATE = 1,
    SNAPSHOT_DELETE = 2,
    SNAPSHOT_ROLLBACK = 3,
};
typedef journal_event_type journal_event_type_t;
#pragma pack()

typedef uint64_t block_t;
/*meta data store in rocksdb*/
#define META_DIR  "/var/storage_gateway/meta/"
/*data store in file system*/
#define DATA_DIR  "/var/storage_gateway/data/"

/**********************snapshot**********************/
/*block operation way*/
enum cow_op 
{
    COW_YES = 0, /*need cow */
    COW_NO  = 1, /*noneed cow, direct overlap*/
};
typedef enum cow_op cow_op_t;
/*mininum cow block size*/
#define COW_BLOCK_SIZE (1*1024*1024UL)
/************************backup**********************/
/*backup block size*/
#define BACKUP_BLOCK_SIZE (1*1024*1024UL)

#endif
