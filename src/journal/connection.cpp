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

Connection::Connection(raw_socket& socket_, 
                       entry_queue& entry_queue,std::condition_variable& entry_cv,
					   reply_queue& reply_queue,std::condition_variable& reply_cv,
                       BlockingQueue<struct IOHookRequest>& read_queue)
                        :raw_socket_(socket_),
                         entry_queue_(entry_queue),
                         entry_cv_(entry_cv),
                         reply_queue_(reply_queue),
                         reply_cv_(reply_cv),
                         read_queue_(read_queue),
                         buffer_pool(NULL)
{
}

Connection::~Connection()
{

}

bool Connection::init(nedalloc::nedpool * buffer)
{
    buffer_pool = buffer;
    thread_ptr.reset(new boost::thread(boost::bind(&Connection::send_thread, this)));
    return true;
}

bool Connection::deinit()
{
    thread_ptr->interrupt();
    thread_ptr->join();
    return true;
}

void Connection::start()
{
    raw_socket_.set_option(boost::asio::ip::tcp::no_delay(true));
    read_request_header();
}

void Connection::stop()
{
    boost::system::error_code ignored_ec;
    #ifndef _USE_UNIX_DOMAIN
    raw_socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both,ignored_ec);
    #else
    raw_socket_.shutdown(boost::asio::local::stream_protocol::socket,ignored_ec);
    #endif
}

void Connection::read_request_header()
{
    boost::asio::async_read(raw_socket_,
        boost::asio::buffer(header_buffer_, sizeof(struct IOHookRequest)),
        boost::bind(&Connection::handle_request_header, this,
                     boost::asio::placeholders::error));
}
void Connection::dispatch(IOHookRequest* header_ptr)
{
    switch(header_ptr->type)
    {
        case SCSI_READ:
            read_queue_.push(*header_ptr);
            read_request_header();
            break;
        case SCSI_WRITE:
            parse_write_request(header_ptr);
            break;
        case SYNC_CACHE:
            //send_reply(true);
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
        std::cerr << "xxxxxxx" << e <<std::endl;
        std::cerr << "magic:" << header_ptr->magic << " X:" << MESSAGE_MAGIC << std::endl;
        std::cerr << "request type:" << header_ptr->type << std::endl;
        std::cerr << "request len " << header_ptr->len << std::endl;
        read_request_header();

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
                    boost::asio::buffer(buffer_ptr+header_size, header_ptr->len),
                    boost::bind(&Connection::handle_write_request_body, this,buffer_ptr,buffer_size,
                    boost::asio::placeholders::error));
        }
        else
        {
            //todo block or remalloc
            ;
            std::cerr << "buffer_ptr is NULL" << std::endl;
        }
    
    }
    else
    {
        //todo just a header?
        ;
        std::cerr <<"write header_ptr->len :" << header_ptr << std::endl;
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

    }
    else
    {
        //todo bad request
        ;
    }
    read_request_header();
}   

bool Connection::handle_write_request(char* buffer,uint32_t size,char* header)
{     
    if (buffer == NULL || header == NULL)
    {
        //LOG
        return false;
    }

    log_header_t* buffer_ptr = reinterpret_cast<log_header_t*>(buffer);
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest*>(header);
    off_len_t* off_ptr = reinterpret_cast<off_len_t *>(buffer + sizeof(log_header_t));
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

void Connection::send_thread()
{
    IOHookReply* reply = NULL;
    while(true)
    {
        std::unique_lock<std::mutex> lk(mtx_);
        while(reply_queue_.empty())
        {
            reply_cv_.wait(lk);
        }
        cout << "send_thread triggered" << endl;
        if(!reply_queue_.pop(reply))
        {
            LOG_ERROR << "reply_queue pop failed";
            continue;
        }
        send_reply(reply);
    }
}

void Connection::send_reply(IOHookReply* reply)
{
    if(NULL == reply)
    {
        LOG_ERROR << "Invalid reply ptr";
        return;
    }
    
    cout << "send reply magic:" << reply->magic << endl;
    boost::asio::async_write(raw_socket_,
    boost::asio::buffer(reply, sizeof(struct IOHookReply)),
    boost::bind(&Connection::handle_send_reply, this,reply,
                 boost::asio::placeholders::error));
}

void Connection::handle_send_reply(IOHookReply* reply,const boost::system::error_code& err)
{
    if (!err)
    {
        if(reply->len > 0)
        {
            cout << "send reply ack len:" << reply->len << endl;
                boost::asio::async_write(raw_socket_,boost::asio::buffer(reply->data, reply->len),
                boost::bind(&Connection::handle_send_data, this,reply,
                boost::asio::placeholders::error));
        }
        else
        {
            delete []reply;
        }
    }
    else
    {
        LOG_ERROR << "send reply failed,reply id:" << reply->handle;
        delete []reply;
    }

}

void Connection::handle_send_data(IOHookReply* reply,const boost::system::error_code& err)
{
    if(err)
    {
        //LOG_ERROR << "send reply data failed, reply id:" << reply->handle;
        cout << "send reply data failed, reply id:" << reply->handle;
    }
    cout << "send reply data ok" << endl;
    delete []reply;
}
}
