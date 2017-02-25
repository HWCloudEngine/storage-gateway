#include "connection.h"
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

#include "common/define.h"
#include "rpc/message.pb.h"

using huawei::proto::WriteMessage;
using huawei::proto::DiskPos;

namespace Journal{

Connection::Connection(raw_socket_t& socket_, 
                       BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                       BlockingQueue<struct IOHookRequest>& read_queue,
                       BlockingQueue<struct IOHookReply*>&  reply_queue)
                        :raw_socket_(socket_),
                         entry_queue_(entry_queue),
                         read_queue_(read_queue),
                         reply_queue_(reply_queue),
                         buffer_pool(NULL)
{
}

Connection::~Connection()
{

}

bool Connection::init(nedalloc::nedpool* buffer)
{
    buffer_pool = buffer;
    running_flag = true;
    req_seq = 0;
    reply_thread_.reset(new thread(bind(&Connection::send_thread, this)));
    return true;
}

bool Connection::deinit()
{
    running_flag = false;
    reply_thread_->join();
    return true;
}

void Connection::start()
{
    #ifndef _USE_UNIX_DOMAIN
    raw_socket_->set_option(boost::asio::ip::tcp::no_delay(true));
    #endif

    /*read message head*/
    read_request_header();
}

void Connection::stop()
{
    boost::system::error_code ignored_ec;
    #ifndef _USE_UNIX_DOMAIN
    raw_socket_->shutdown(boost::asio::ip::tcp::socket::shutdown_both,ignored_ec);
    #else
    raw_socket_->shutdown(boost::asio::local::stream_protocol::socket::shutdown_both,
                          ignored_ec);
    #endif
}

void Connection::read_request_header()
{
    /*asio read IoHookRequest and store into header_buffer_*/
    boost::asio::async_read(*raw_socket_,
      boost::asio::buffer(header_buffer_, sizeof(struct IOHookRequest)),
        boost::bind(&Connection::handle_request_header, this,
                     boost::asio::placeholders::error));
}

void Connection::dispatch(IOHookRequest* header_ptr)
{
    switch(header_ptr->type)
    {
        case SCSI_READ:
            /*request to journal reader*/
            read_queue_.push(*header_ptr);
            read_request_header();
            break;
        case SCSI_WRITE:
            /*requst to journal writer*/
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
        /*dispath IoHookRequest to Journal Writer or Reader*/
        dispatch(header_ptr);
    }
    else
    {
        LOG_ERROR << "recieve header error:" << e << " ,magic number:" << header_ptr->magic;
    }
}

void Connection::parse_write_request(IOHookRequest* header_ptr)
{
    if (header_ptr->len > 0)
    {
        /*receive write data*/
        uint32_t buffer_size = header_ptr->len ;
        char*    buffer_ptr  = NULL;
        uint32_t try_attempts = 0;
        while(buffer_ptr == NULL)
        {
            if(try_attempts > 0)
            {
                boost::this_thread::sleep_for(boost::chrono::milliseconds(500));
            }
            buffer_ptr = reinterpret_cast<char*>(nedalloc::nedpmalloc(buffer_pool,buffer_size));
            try_attempts++;
        }

        if (buffer_ptr!=NULL)
        {
            /*asio read write data*/
            boost::asio::async_read(*raw_socket_,
                boost::asio::buffer(buffer_ptr, buffer_size),
                    boost::bind(&Connection::handle_write_request_body, 
                                this, 
                                buffer_ptr,buffer_size,
                                boost::asio::placeholders::error));
            return;
        }
    }
    else
    {
        LOG_ERROR << "write request data length == 0";
    }

    read_request_header();
}


void Connection::handle_write_request_body(char* buffer_ptr,uint32_t buffer_size,
                                           const boost::system::error_code& e)
{
    if(!e)
    {
        bool ret = handle_write_request(buffer_ptr, buffer_size,header_buffer_.data());
        if (!ret)
        {
            LOG_ERROR << "handle write request failed";
        }
    }
    else
    {
        LOG_ERROR << "recieve write request data error:" << e;
    }
    read_request_header();
}   

bool Connection::handle_write_request(char* buffer, uint32_t size, char* header)
{     
    if (buffer == NULL || header == NULL)
    {
        return false;
    }
    
    IOHookRequest* header_ptr = reinterpret_cast<IOHookRequest*>(header);
    char*  write_data  = buffer;
    size_t write_data_len = size;
    
    /*spawn write message*/
    shared_ptr<WriteMessage> message = make_shared<WriteMessage>();
    DiskPos* pos = message->add_pos(); 
    pos->set_offset(header_ptr->offset);
    pos->set_length(header_ptr->len);
    /*todo: optimize reduce data copy*/
    message->set_data(write_data, write_data_len);
   
    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = make_shared<JournalEntry>();
    entry->set_handle(header_ptr->handle);
    entry->set_sequence(req_seq);
    req_seq++;
    entry->set_type(IO_WRITE);
    entry->set_message(message);
    
    /*enqueue*/
    entry_queue_.push(entry);
   
    /*free buffer, data already copy to WriteMessage*/
    nedalloc::nedpfree(buffer_pool, buffer);
    return true;
}

void Connection::send_thread()
{
    IOHookReply* reply = NULL;
    while(running_flag)
    {
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
    boost::asio::async_write(*raw_socket_,
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
            boost::asio::async_write(*raw_socket_,boost::asio::buffer(reply->data, reply->len),
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
        LOG_ERROR << "send reply data failed, reply id:" << reply->handle;
    }
    delete []reply;
}
}
