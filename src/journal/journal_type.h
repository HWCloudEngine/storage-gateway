#ifndef __JOURNAL_TYPE_H
#define __JOURNAL_TYPE_H 

#include <stdint.h> 
/*crc type*/ 
typedef enum {
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

/*journal event type*/
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

#endif
