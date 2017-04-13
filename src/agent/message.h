#ifndef MESSAGE_H
#define MESSAGE_H

#define MSG_MAGIC (0xAA)
#define MAX_VOL_NAME (128)
#define MAX_DEV_PATH (128)

struct HookRequest
{
    uint32_t magic;
    uint32_t type;
    uint32_t reserves;
    uint64_t handle;
    uint64_t offset;
    uint32_t len;
    uint8_t  data[0];
}__attribute__((packed));

struct HookReply
{
    uint32_t magic;
    uint32_t error;
    uint32_t reserves;
    uint64_t handle;
    uint32_t len;
    uint8_t  data[0];
}__attribute__((packed));

enum HookCmdType
{
    ADD_VOLUME = 0,
    DEL_VOLUME = 1,
    IO_READ    = 3,
    IO_WRITE   = 4 
};

struct add_vol_req 
{
    char vol_name[MAX_VOL_NAME];
    char dev_path[MAX_DEV_PATH];
};

typedef struct add_vol_req add_vol_req_t;

struct del_vol_req
{
    char vol_name[MAX_VOL_NAME];
};

typedef struct del_vol_req del_vol_req_t;

#endif
