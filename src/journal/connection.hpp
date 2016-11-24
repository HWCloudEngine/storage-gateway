#ifndef JOURNAL_CONNECTION_HPP
#define JOURNAL_CONNECTION_HPP

#include <mutex>
#include <condition_variable>
#include <chrono>
#include <iostream>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/array.hpp>
#include <boost/thread/thread.hpp>
#include "../common/blocking_queue.h"
#include "../common/prqueue.h"
#include "message.hpp"
#include "journal_entry.hpp"
#include "nedmalloc.h"
#include "seq_generator.hpp"
#include "cache/cache_proxy.h"

#ifndef _USE_UNIX_DOMAIN
typedef boost::asio::ip::tcp::socket raw_socket;
#else
typedef boost::asio::local::stream_protocol::socket raw_socket;  
#endif

namespace Journal{

class Connection : public boost::enable_shared_from_this<Connection>,
                   private boost::noncopyable
{
public:
    explicit Connection(raw_socket& socket_,
                        PRQueue<shared_ptr<JournalEntry>>&   entry_queue,
                        BlockingQueue<struct IOHookRequest>& read_queue,
                        BlockingQueue<struct IOHookReply*>&  reply_queue);
    virtual ~Connection();
    bool init(nedalloc::nedpool * buffer);
    bool deinit();
    void start();
    void stop();

private:
    void handle_request_header(const boost::system::error_code& error);
    void handle_write_request_body(char* buffer_ptr,uint32_t buffer_size,
                                   const boost::system::error_code& e);
    bool handle_write_request(char* buffer,uint32_t size,char* header);
    void parse_write_request(IOHookRequest* header_ptr);
    void dispatch(IOHookRequest* header_ptr);
    void read_request_header();
    void send_thread();
    void send_reply(IOHookReply* reply);
    void handle_send_reply(IOHookReply* reply,const boost::system::error_code& err);
    void handle_send_data(IOHookReply* reply,const boost::system::error_code& err);
    
    /*socket*/
    raw_socket& raw_socket_;
    
    /*write io input queue*/
    PRQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*read io input queue*/
    BlockingQueue<struct IOHookRequest>& read_queue_;
    /*output queue*/
    BlockingQueue<struct IOHookReply*>& reply_queue_;
    
    /*mem pool*/
    boost::array<char, HEADER_SIZE> header_buffer_;
    nedalloc::nedpool * buffer_pool;
    
    /*reply thread*/
    bool running_flag;
    boost::shared_ptr<boost::thread> reply_thread_;
    
    /*internal request sequence, use for multi thread keep order*/
    uint64_t req_seq;
};

typedef boost::shared_ptr<Connection> connection_ptr;
}

#endif
