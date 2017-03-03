#ifndef JOURNAL_CONNECTION_H
#define JOURNAL_CONNECTION_H

#include <mutex>
#include <thread>
#include <memory>
#include <condition_variable>
#include <chrono>
#include <iostream>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include "../common/blocking_queue.h"
#include "message.h"
#include "journal_entry.h"
#include "nedmalloc.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"

using namespace std;

#ifndef _USE_UNIX_DOMAIN
typedef boost::asio::ip::tcp::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#else
typedef boost::asio::local::stream_protocol::socket socket_t ;
typedef shared_ptr<socket_t> raw_socket_t;  
#endif

namespace Journal{

class Connection 
{
public:
    explicit Connection(raw_socket_t& socket_,
                        BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                        BlockingQueue<struct IOHookRequest>& read_queue,
                        BlockingQueue<struct IOHookReply*>&  reply_queue);
    virtual ~Connection();

    Connection(const Connection& c) = delete;
    Connection& operator=(const Connection& c) = delete;

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
    raw_socket_t& raw_socket_;
    
    /*write io input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*read io input queue*/
    BlockingQueue<struct IOHookRequest>& read_queue_;
    /*output queue*/
    BlockingQueue<struct IOHookReply*>& reply_queue_;
    
    /*mem pool*/
    boost::array<char, HEADER_SIZE> header_buffer_;
    nedalloc::nedpool * buffer_pool;
    
    /*reply thread*/
    bool running_flag;
    shared_ptr<thread> reply_thread_;
    
    /*internal request sequence, use for multi thread keep order*/
    uint64_t req_seq;
};

}

#endif
