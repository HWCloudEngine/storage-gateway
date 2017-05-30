/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    client_socket.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: network read and send module 
*
*************************************************/
#include <string>
#include <stdexcept>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include "common/define.h"
#include "rpc/message.pb.h"
#include "perf_counter.h"
#include "volume_manager.h"
#include "client_socket.h"

using boost::asio::buffer;
using huawei::proto::WriteMessage;
using huawei::proto::DiskPos;

ClientSocket::ClientSocket(VolumeManager& vol_manager,
                     raw_socket_t& socket_,
                     BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                     BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                     BlockingQueue<io_request_t>& read_queue,
                     BlockingQueue<io_reply_t*>&  reply_queue)
                    : vol_manager_(vol_manager), raw_socket_(socket_),
                      entry_queue_(entry_queue), write_queue_(write_queue),
                      read_queue_(read_queue), reply_queue_(reply_queue) {
    LOG_INFO << "Network work thread create";
}

ClientSocket::~ClientSocket() {
    LOG_INFO << "NetWork work thread destroy";
}

bool ClientSocket::init() {
    recv_buf_ = new char[recv_buf_len_];
    if (!recv_buf_) {
        LOG_ERROR << "allocate recv buf failed";
    }
    memset(recv_buf_, 0, recv_buf_len_);
    running_flag = true;
    recv_thread_.reset(new thread(bind(&ClientSocket::recv_thread, this)));
    reply_thread_.reset(new thread(bind(&ClientSocket::send_thread, this)));
    return true;
}

bool ClientSocket::deinit() {
    if (recv_buf_) {
        delete [] recv_buf_;
    }
    running_flag = false;
    recv_thread_->join();
    reply_queue_.stop();
    reply_thread_->join();
    return true;
}

void ClientSocket::start() {
    #ifndef _USE_UNIX_DOMAIN
    raw_socket_->set_option(boost::asio::ip::tcp::no_delay(true));
    #endif
}

void ClientSocket::stop() {
    LOG_INFO << "connection stop";
    boost::system::error_code ignored_ec;
    #ifndef _USE_UNIX_DOMAIN
    raw_socket_->shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                          ignored_ec);
    #else
    raw_socket_->shutdown(
        boost::asio::local::stream_protocol::socket::shutdown_both, ignored_ec);
    #endif
    LOG_INFO << "connection stop ok";
}

bool ClientSocket::recv_request(io_request_t* req) {
    boost::system::error_code ec;
    size_t read_ret = boost::asio::read(*raw_socket_, buffer(req, sizeof(*req)), ec);
    if (!ec && ec == boost::asio::error::eof) {
        LOG_INFO << "socket eof";
        return false;
    }
    if (read_ret != sizeof(*req)) {
        LOG_ERROR << "read request failed size: " << sizeof(*req)
                  << " ret:" << read_ret;
        return false;
    }
    if (req->magic != MESSAGE_MAGIC) {
        LOG_ERROR << "read request format failed";
        return false;
    }

    if (req->type != SCSI_READ && req->len > 0) {
        assert(req->len <= recv_buf_len_);
        read_ret = boost::asio::read(*raw_socket_, buffer(recv_buf_, req->len));
        if (read_ret != req->len) {
            LOG_ERROR << "read request body failed size: " << req->len 
                      << " ret:" << read_ret;
            return false;
        }
    }
    return true;
}

void ClientSocket::recv_thread() {
    while (running_flag) {
        io_request_t head = {0}; 
        bool ret = recv_request(&head);
        if (!ret) {
            LOG_ERROR << "recv_req failed";
            break;
        }
        pre_perf(head.seq, (uint8_t)head.type, head.offset, head.len);
        do_perf(RECV_BEGIN, head.seq);
        dispatch(&head);
        do_perf(RECV_END, head.seq);
    }
}

void ClientSocket::dispatch(const io_request_t* head) {
    switch (head->type) {
        case SCSI_READ:
            /*request to journal reader*/
            handle_read_req(head);
            break;
        case SCSI_WRITE:
            /*requst to journal writer*/
            handle_write_req(head);
            break;
        case SYNC_CACHE:
            handle_flush_req(head);
            break;
        case DEL_VOLUME:
            handle_delete_req(head);
            break;
        default:
            LOG_ERROR << "unsupported request type:" << head->type;
    }
}

void ClientSocket::handle_read_req(const io_request_t* req) {
    read_queue_.push(*req);
}

void ClientSocket::handle_write_req(const io_request_t* req) {
    /*spawn write message*/
    shared_ptr<WriteMessage> message = make_shared<WriteMessage>();
    DiskPos* pos = message->add_pos();
    pos->set_offset(req->offset);
    pos->set_length(req->len);
    /*todo: optimize reduce data copy*/
    message->set_data(recv_buf_, req->len);
    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = make_shared<JournalEntry>();
    entry->set_sequence(req->seq);
    entry->set_handle(req->handle);
    entry->set_type(IO_WRITE);
    entry->set_message(message);
    /*enqueue*/
    entry_queue_.push(entry);
}

void ClientSocket::handle_flush_req(const io_request_t* req) {
    io_reply_t reply;
    memset(&reply, 0, sizeof(reply));
    reply.magic = MESSAGE_MAGIC;
    reply.handle = req->handle;
    
    LOG_INFO << "flush command";
    while (!entry_queue_.empty() && !write_queue_.empty()) {
        LOG_INFO << "flush current queue is not empty, waiting";
        usleep(100);
    }
    LOG_INFO << "flush command ok";
    boost::asio::write(*raw_socket_, boost::asio::buffer(&reply,
                       sizeof(io_reply_t)));
}

void ClientSocket::handle_delete_req(const io_request_t* req) {
    assert(req->type == DEL_VOLUME);
    del_vol_req_t* del_req = reinterpret_cast<del_vol_req_t*>(recv_buf_);

    /*send reply to tgt*/
    io_reply_t reply;
    memset(&reply, 0, sizeof(reply));
    reply.magic = MESSAGE_MAGIC;
    boost::asio::write(*raw_socket_, boost::asio::buffer(&reply, sizeof(io_reply_t)));
    std::string vol_name(del_req->volume_name);
    LOG_INFO << "delete volume :" << vol_name;
    LOG_INFO << "delete volume :" << vol_name << " ok";
}

void ClientSocket::send_thread() {
    io_reply_t* reply = NULL;
    while (running_flag) {
       if (!reply_queue_.pop(reply)) {
            LOG_ERROR << "reply_queue pop failed";
            break;
        }
        send_reply(reply);
    }
}

void ClientSocket::send_reply(const io_reply_t* reply) {
    if (NULL == reply) {
        LOG_ERROR << "Invalid reply ptr";
        return;
    }
    do_perf(REPLY_BEGIN, reply->seq);
    size_t write_ret = raw_socket_->write_some(buffer(reply, sizeof(*reply)));
    if (write_ret != sizeof(*reply)) {
        LOG_ERROR << "reply head failed size:" << sizeof(*reply) << " ret:" << write_ret;
        return;
    }
    if (reply->len > 0) {
        write_ret = raw_socket_->write_some(buffer(reply->data, reply->len)); 
        if (write_ret != reply->len) {
            LOG_ERROR << "reply data failed size:" << reply->len << " ret:" << write_ret;
            return;
        }
    }
    do_perf(REPLY_END, reply->seq);
    post_perf(reply->seq);
    delete []reply;
}
