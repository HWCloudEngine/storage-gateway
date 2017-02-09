/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_transmitter.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_CLIENT_H_
#define REP_CLIENT_H_
#include <list>
#include <vector>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "rpc/transfer.grpc.pb.h"
#include "rep_type.h"
#include "common/thread_pool.h"
using huawei::proto::JournalMarker;
using grpc::Channel;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
typedef grpc_connectivity_state ClientState;
#define CLIENT_IDLE         GRPC_CHANNEL_IDLE
#define CLIENT_CONNECTING   GRPC_CHANNEL_CONNECTING
#define CLIENT_READY        GRPC_CHANNEL_READY
#define CLIENT_FAILURE      GRPC_CHANNEL_TRANSIENT_FAILURE
#define CLIENT_SHUTDOWN     GRPC_CHANNEL_SHUTDOWN

class Transmitter{
private:
    bool done_;
    std::atomic<uint64_t> seq_id_;
    std::mutex mtx_;
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<huawei::proto::transfer::DataTransfer::Stub> stub_;
    std::unique_ptr<sg_threads::ThreadPool> task_pool_;
    //queue of task which need to be excuted
    BlockingQueue<std::shared_ptr<RepTask>> in_task_que_;
    //queue of task which need to be recycled
    BlockingQueue<std::shared_ptr<RepTask>> out_task_que_;
    std::thread dispatch_thread_;
public:
    static Transmitter& instance(){
        static Transmitter t;
        return t;
    }
    Transmitter(Transmitter&) = delete;
    Transmitter& operator=(Transmitter const&) = delete;
    void init(std::shared_ptr<Channel> channel,int max_tasks);
    // submit task to in task que
    bool submit_task(std::shared_ptr<RepTask> task);
    // sync remote volume's producer marker
    bool sync_marker(const std::string& vol,const JournalMarker& marker);
    // get rpc state
    ClientState get_state(bool try_to_connect);
    bool wait_for_state_change(const ClientState& state,
            std::chrono::system_clock::time_point deadline);
    std::string get_printful_state(ClientState state);
private:
    Transmitter();
    ~Transmitter();
    void do_transfer(std::shared_ptr<RepTask> task);
    void dispatch();
};

#endif

