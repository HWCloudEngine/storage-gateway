#ifndef JOURNAL_MESSAGE_HPP
#define JOURNAL_MESSAGE_HPP

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "nedmalloc.h"

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
    ReplayEntry(char* data,uint32_t size):data_(data),data_size_(size)
    {
    }
    virtual ~ReplayEntry()
    {
        //todo use nedpfree here or when we pop ReplayEntry from entry_queue?
    }

    const char* body()const
    {
        return data_ + sizeof(struct Request);
    }

    char* data()
    {
        return data_;
    }

    uint32_t length()const
    {
        return data_size_;
    }
      
    uint32_t body_lenth()const
    {
        return data_size_ - sizeof(struct Request);
    }
      
private:
    char* data_;
    uint32_t data_size_;
};
typedef ReplayEntry* entry_ptr;

}
#endif
