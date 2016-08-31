#include "connection.hpp"
#include <boost/bind.hpp>

#if !defined(USE_NEDMALLOC_DLL)
#include "nedmalloc.c"
#elif defined(WIN32)
#define WIN32_LEAN_AND_MEAN 1
#include <windows.h>
#endif

#ifdef _MSC_VER
#define MEMALIGNED(v) __declspec(align(v))
#elif defined(__GNUC__)
#define MEMALIGNED(v) __attribute__ ((aligned(v)))
#else
#define MEMALIGNED(v)
#endif


namespace Journal{

Connection::Connection(raw_socket & socket_, entry_queue& entry_queue,std::condition_variable& entry_cv)
                        :raw_socket_(socket_),
                         entry_queue_(entry_queue),
                         entry_cv_(entry_cv),
                         buffer_pool(NULL)
{
    
}

Connection::~Connection()
{

}

bool Connection::init(nedalloc::nedpool * buffer)
{
    buffer_pool = buffer;
    return true;
}

bool Connection::deinit()
{
    //todo
    return true;
}

void Connection::start()
{
    boost::asio::async_read(raw_socket_,
        boost::asio::buffer(header_buffer_, sizeof(struct IOHookRequest)),
        boost::bind(&Connection::handle_request_header, shared_from_this(),
                     boost::asio::placeholders::error));
    
}

void Connection::stop()
{
    boost::system::error_code ignored_ec;
    raw_socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both,ignored_ec);
}

void Connection::dispatch(IOHookRequest* header_ptr)
{
    switch(header_ptr->type)
    {
        case SCSI_READ:
            //todo
            break;
        case SCSI_WRITE:
            parse_write_request(header_ptr);
            break;
        case SYNC_CACHE:
            //todo
            break;
        default:
            LOG_ERROR << "unsupported request type:" << header_ptr->type;
    }
}

void Connection::handle_request_header(const boost::system::error_code& e)
{
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest *>(header_buffer_.data());
    if(!e && header_ptr->magic == MESSAGE_MAGIC )
    {
        dispatch(header_ptr);
    }
    else
    {
        //todo bad request
        ;
    }

}

void Connection::parse_write_request(IOHookRequest* header_ptr)
{
    if (header_ptr->len > 0)
    {
        uint32_t header_size = sizeof(log_header_t) + sizeof(off_len_t);
        uint32_t buffer_size = header_ptr->len + header_size;
        // buffer_ptr will be free when we finish journal write,use nedpfree
        char* buffer_ptr = reinterpret_cast<char *>(nedalloc::nedpmalloc(buffer_pool,buffer_size));
        if (buffer_ptr!=NULL)
        {
            boost::asio::async_read(raw_socket_,
                    boost::asio::buffer(buffer_ptr+header_size, buffer_size),
                    boost::bind(&Connection::handle_write_request_body, shared_from_this(),buffer_ptr,buffer_size,
                    boost::asio::placeholders::error));
        }
        else
        {
            //todo block or remalloc
            ;
        }
    
    }
    else
    {
        //todo just a header?
        ;
    }

}


void Connection::handle_write_request_body(char* buffer_ptr,uint32_t buffer_size,const boost::system::error_code& e)
{
    if(!e)
    {
        bool ret = handle_write_request(buffer_ptr,buffer_size,header_buffer_.data());
        if (!ret)
        {
            ;
            //todo reply client error code
        }
        boost::asio::async_read(raw_socket_,
            boost::asio::buffer(header_buffer_, sizeof(struct IOHookRequest)),
            boost::bind(&Connection::handle_request_header, shared_from_this(),
            boost::asio::placeholders::error));
    }
    else
    {
        //todo bad request
        ;
    }

}   

bool Connection::handle_write_request(char* buffer,uint32_t size,char* header)
{     
    if (buffer == NULL || header == NULL)
    {
        //LOG
        return false;
    }
    log_header_t* buffer_ptr = reinterpret_cast<log_header_t *>(buffer_ptr);
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest *>(header);
    off_len_t* off_ptr = reinterpret_cast<off_len_t *>(buffer_ptr + sizeof(log_header_t));
    buffer_ptr->type = LOG_IO;
    //merge will change the count and offset,maybe should remalloc
    buffer_ptr->count = 1;
    off_ptr->length = header_ptr->len;
    off_ptr->offset = header_ptr->offset;
        
    entry_ptr entry_ptr_ = NULL;
    try
    {
        entry_ptr_= new ReplayEntry(buffer,size,header_ptr->handle,buffer_pool);
    }
    catch(const std::bad_alloc & e)
    {
        //LOG
        return false;
    }
    if(entry_ptr_ == NULL)
    {
        //LOG
        return false;
    }
    if(!entry_queue_.push(entry_ptr_))
    {
        //todo LOG
        delete entry_ptr_;
        return false;
    }
    entry_cv_.notify_all();
    return true;
}

}
