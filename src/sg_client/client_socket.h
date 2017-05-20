/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    client_socket.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  network recv and send module
*
*************************************************/
#ifndef SRC_SG_CLIENT_SOCKET_H_
#define SRC_SG_CLIENT_SOCKET_H_
#include <iostream>
#include <string>
#include <mutex>
#include <thread>
#include <memory>
#include <vector>
#include <chrono>
#include <condition_variable>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include "common/blocking_queue.h"
#include "common/journal_entry.h"
#include "message.h"
#include "seq_generator.h"

#ifndef _USE_UNIX_DOMAIN
typedef boost::asio::ip::tcp::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#else
typedef boost::asio::local::stream_protocol::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#endif

class VolumeManager;

/*each client socket*/
class ClientSocket {
 public:
    explicit ClientSocket(VolumeManager& vol_manager,
                          raw_socket_t& socket_,
                          BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                          BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                          BlockingQueue<io_request_t>& read_queue,
                          BlockingQueue<io_reply_t*>& reply_queue);
    ClientSocket(const ClientSocket& sock) = delete;
    ClientSocket& operator=(const ClientSocket& sock) = delete;
    virtual ~ClientSocket();

    bool init();
    bool deinit();
    void start();
    void stop();

 private:
    void recv_thread();
    void send_thread();
    bool recv_request(io_request_t* req);
    void send_reply(const io_reply_t* rep);
    void dispatch(const io_request_t* req);
    void handle_read_req(const io_request_t* req);
    void handle_write_req(const io_request_t* req);
    void handle_delete_req(const io_request_t* req);
    void handle_flush_req(const io_request_t* req);

    /*all volume group*/
    VolumeManager& vol_manager_;
    /*socket*/
    raw_socket_t& raw_socket_;
    /*entry input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*write input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
    /*read input queue*/
    BlockingQueue<io_request_t>& read_queue_;
    /*output queue*/
    BlockingQueue<io_reply_t*>& reply_queue_;
    /*mem pool*/
    char*  recv_buf_;
    static constexpr size_t recv_buf_len_ = (8*1024*1024);
    /*recv and send thread*/
    bool running_flag;
    shared_ptr<thread> recv_thread_;
    shared_ptr<thread> reply_thread_;
};

#endif  // SRC_SG_CLIENT_SOCKET_H_
