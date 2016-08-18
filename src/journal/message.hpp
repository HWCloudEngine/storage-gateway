#ifndef JOURNAL_MESSAGE_HPP
#define JOURNAL_MESSAGE_HPP

#define MESSAGE_MAGIC (0xAA)

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

}
#endif
