#ifndef JOURNAL_REPLAY_ENTRY_HPP
#define JOURNAL_REPLAY_ENTRY_HPP

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/asio.hpp>

#include "nedmalloc.h"
#include "../common/log_header.h"
#include "../log/log.h"
#include "message.hpp"

namespace Journal{

#ifndef _USE_UNIX_DOMAIN
typedef boost::asio::ip::tcp::socket raw_socket;
#else
typedef boost::asio::local::stream_protocol::socket raw_socket;  
#endif

typedef boost::lockfree::queue<IOHookReply*> reply_queue;

class ReplayEntry
    :public boost::enable_shared_from_this<ReplayEntry>,
     private boost::noncopyable
{
public:
    ReplayEntry(char* data,uint32_t size,uint64_t req_id,nedalloc::nedpool * buffer_pool)
        :data_(data),data_size_(size),req_id_(req_id),buffer_pool_(buffer_pool)
    {
    }
    virtual ~ReplayEntry()
    {
        if(data_ != NULL)
        {
            nedalloc::nedpfree(buffer_pool_,data_);
            data_ = NULL;
        }
    }

    const char* body()const
    {
        return data_ + header_length();
    }

    const uint32_t body_length()const
    {
        return data_size_ - header_length();
    }

    const char* data()const
    {
        return data_;
    }

    const uint32_t length()const
    {
        return data_size_;
    }

    log_header_t* header()
    {
        log_header_t* header_ptr = reinterpret_cast<log_header_t *>(data_);
        return header_ptr;
    }
      
    const uint32_t header_length()const
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
    nedalloc::nedpool * buffer_pool_;
};
typedef ReplayEntry* entry_ptr;
typedef boost::lockfree::queue<entry_ptr> entry_queue;
}
#endif
