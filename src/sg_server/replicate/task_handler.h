/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    task_handler.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef TASK_HANDLER_H_
#define TASK_HANDLER_H_
#include <list>
#include <vector>
#include <mutex>
#include <thread>
#include <functional>
#include "rep_type.h"
#include "common/thread_pool.h"
#include "replicator_context.h"
#include "rep_task.h"
#include "sg_server/transfer/net_sender.h"
using huawei::proto::JournalMarker;

class TaskHandler{
private:
    bool running_;
    std::atomic<uint64_t> seq_id_;
    std::mutex mtx_;
    //input, queue of task which need to be excuted
    std::shared_ptr<BlockingQueue<std::shared_ptr<TransferTask>>> in_task_que_;
    //output, queue of markerContext which need be synced
    std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> out_que_;
    std::unique_ptr<sg_threads::ThreadPool> tp_;
public:
    static TaskHandler& instance(){
        static TaskHandler t;
        return t;
    }
    TaskHandler(TaskHandler&) = delete;
    TaskHandler& operator=(TaskHandler const&) = delete;
    void init(
            std::shared_ptr<BlockingQueue<std::shared_ptr<TransferTask>>> in,
            std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> out);

    // add markerContext to output queue
    int add_marker_context(ReplicatorContext* rep_ctx,
            const JournalMarker& marker);
private:
    TaskHandler();
    ~TaskHandler();

    void do_transfer(std::shared_ptr<TransferTask> task,
            grpc_stream_ptr& stream);

    // thread main loop method
    void work();
};

#endif

