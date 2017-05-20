#ifndef JOURNAL_MESSAGE_H
#define JOURNAL_MESSAGE_H

#define MESSAGE_MAGIC (0xAA)
#define MAX_VOLUME_NAME (128)
#define MAX_DEVICE_PATH (256)
#define HEADER_SIZE (128)

struct io_request {
    uint32_t magic;
    uint32_t type;          /*command type*/
    uint64_t seq;
    uint64_t handle;        /*command unique identifier*/
    uint64_t offset;
    uint32_t len;
    uint8_t  data[0];
}__attribute__((packed));
typedef struct io_request io_request_t;

struct io_reply {
    uint32_t magic;
    uint32_t error;
    uint64_t seq;
    uint64_t handle;
    uint32_t len;
    uint8_t  data[0];
}__attribute__((packed));
typedef struct io_reply io_reply_t;

enum io_request_code {
    ADD_VOLUME = 0,
    DEL_VOLUME = 1,

    SCSI_READ  = 3,   /*scsi read command*/
    SCSI_WRITE = 4,   /*scsi write command*/
    SYNC_CACHE = 5    /*synchronize cache when iscsi initiator logout*/ 
};
typedef enum io_request_code io_request_code_t;

struct add_vol_req{
    char volume_name[MAX_VOLUME_NAME];
    char device_path[MAX_DEVICE_PATH];
};
typedef struct add_vol_req add_vol_req_t;

struct del_vol_req{
    char volume_name[MAX_VOLUME_NAME];
};
typedef struct del_vol_req del_vol_req_t;

#endif
