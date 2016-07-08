#ifndef JOURNAL_MESSAGE_HPP
#define JOURNAL_MESSAGE_HPP

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/lockfree/queue.hpp>  

#include "nedmalloc.h"
#include "../common/log_header.h"

namespace Journal{

struct Request {
    uint32_t magic;
    uint32_t type;          /*command type*/
    uint32_t reserves;
    uint64_t handle;        /*command unique identifier*/
    uint32_t offset;
    uint32_t len;
}__attribute__((packed));


struct Reply {
    uint32_t magic;
    uint32_t error;
    uint32_t reserves;
    uint64_t handle;
    uint32_t len;
}__attribute__((packed));

class ReplayEntry
    :public boost::enable_shared_from_this<ReplayEntry>,
	 private boost::noncopyable
{
public:
    ReplayEntry(char* data,uint32_t size,uint64_t req_id):data_(data),data_size_(size),req_id_(req_id)
    {
    }
    virtual ~ReplayEntry()
    {
        //todo use nedpfree here or when we pop ReplayEntry from entry_queue?
    }

    const char* body()const
    {
        return data_ + header_lenth();
    }

    const char* data()const
    {
        return data_;
    }

    const uint32_t length()const
    {
        return data_size_;
    }
      
    const uint32_t header_lenth()const
    {
        log_header_t* header_ptr = reinterpret_cast<log_header_t *>(data_);
        uint32_t count = header_ptr->count;
        uint32_t size = sizeof(log_header_t) + count * sizeof(off_len_t);
        return size;
    }

    const uint64_t req_id()const
    {
        return req_id_;
    }
      
private:
    char* data_;
    uint32_t data_size_;
    uint64_t req_id_;
};
typedef ReplayEntry* entry_ptr;
typedef boost::lockfree::queue<entry_ptr> entry_queue;


}
#endif
