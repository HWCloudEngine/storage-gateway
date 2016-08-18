#ifndef JOURNAL_MESSAGE_HPP
#define JOURNAL_MESSAGE_HPP

#define MESSAGE_MAGIC (0xAA)
#define MAX_VOLUME_NAME (128)
#define MAX_DEVICE_PATH (256)
#define HEADER_SIZE (128)


namespace Journal{

struct IOHookRequest {
    uint32_t magic;
    uint32_t type;          /*command type*/
    uint32_t reserves;
    uint64_t handle;        /*command unique identifier*/
    uint32_t offset;
    uint32_t len;
}__attribute__((packed));


struct IOHookReply {
    uint32_t magic;
    uint32_t error;
    uint32_t reserves;
    uint64_t handle;
    uint32_t len;
}__attribute__((packed));

enum IOHook_request_code {
    ADD_VOLUME = 0,
    DEL_VOLUME = 1,

    SCSI_READ  = 3,   /*scsi read command*/
    SCSI_WRITE = 4,   /*scsi write command*/
    SYNC_CACHE = 5    /*synchronize cache when iscsi initiator logout*/ 
};
typedef enum IOHook_request_code IOHook_request_code_t;

struct add_vol_req{
    char volume_name[MAX_VOLUME_NAME];
    char device_path[MAX_DEVICE_PATH];
};
typedef struct add_vol_req add_vol_req_t;

struct del_vol_req{
    char volume_name[MAX_VOLUME_NAME];
};
typedef struct del_vol_req del_vol_req_t;

}
#endif
