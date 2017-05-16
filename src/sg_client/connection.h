/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    connection.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  network module
*
*************************************************/
#ifndef SRC_SG_CLIENT_CONNECTION_H_
#define SRC_SG_CLIENT_CONNECTION_H_
#include <iostream>
#include <mutex>
#include <thread>
#include <memory>
#include <condition_variable>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include "common/blocking_queue.h"
#include "common/journal_entry.h"
#include "message.h"
#include "nedmalloc.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"

#ifndef _USE_UNIX_DOMAIN
typedef boost::asio::ip::tcp::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#else
typedef boost::asio::local::stream_protocol::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#endif

namespace Journal {

class VolumeManager;

class Connection {
 public:
    explicit Connection(VolumeManager& vol_manager,
                        raw_socket_t& socket_,
                        BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                        BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                        BlockingQueue<struct IOHookRequest>& read_queue,
                        BlockingQueue<struct IOHookReply*>& reply_queue);
    Connection(const Connection& c) = delete;
    Connection& operator=(const Connection& c) = delete;
    virtual ~Connection();

    bool init(nedalloc::nedpool * buffer);
    bool deinit();
    void start();
    void stop();

 private:
    void handle_request_header(const boost::system::error_code& error);
    void handle_write_request_body(char* buffer_ptr, uint32_t buffer_size,
                                   const boost::system::error_code& e);
    bool handle_write_request(char* buffer, uint32_t size, char* header);
    void parse_write_request(IOHookRequest* header_ptr);
    void dispatch(IOHookRequest* header_ptr);
    void read_request_header();
    void send_thread();
    void send_reply(IOHookReply* reply);
    void handle_send_reply(IOHookReply* reply,
                           const boost::system::error_code& err);
    void handle_send_data(IOHookReply* reply,
                          const boost::system::error_code& err);
    void read_delete_volume_req(const IOHookRequest* req);
    void handle_delete_volume_req(del_vol_req_t* req,
                                  const boost::system::error_code& e);
    void handle_flush_req(const IOHookRequest* req);

    /*all volume group*/
    VolumeManager& vol_manager_;
    /*socket*/
    raw_socket_t& raw_socket_;
    /*entry input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*write input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
    /*read input queue*/
    BlockingQueue<struct IOHookRequest>& read_queue_;
    /*output queue*/
    BlockingQueue<struct IOHookReply*>& reply_queue_;
    /*mem pool*/
    boost::array<char, HEADER_SIZE> header_buffer_;
    nedalloc::nedpool * buffer_pool;
    /*reply thread*/
    bool running_flag;
    shared_ptr<thread> reply_thread_;
};

}  // namespace Journal
#endif  // SRC_SG_CLIENT_CONNECTION_H_
