/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    transfer_task.h
* Author: 
* Date:         2017/02/16
* Version:      1.0
* Description: define a task which will be tranfered to destination
* 
************************************************/
#ifndef TRANSFER_TASK_H_
#define TRANSFER_TASK_H_
#include <atomic>
#include <cstdint>
#include <memory>
#include "rpc/transfer.pb.h"
using huawei::proto::transfer::TransferRequest;

typedef enum TaskStatus{
    T_UNKNOWN,
    T_WAITING,
    T_RUNNING,
    T_DONE,
    T_ERROR
}TaskStatus;

class TaskContext {
public:
    TaskContext() {};
    virtual ~TaskContext() {}
};

class TransferTask{
protected:
    uint64_t id; // task uuid
    std::atomic<TaskStatus> status;
    std::shared_ptr<TaskContext> context;
    uint64_t ts; // timestamp of task, caller may use this to confirm whether it's timeout

public:
    TransferTask(std::shared_ptr<TaskContext> _ctx):
            context(_ctx),
            status(T_UNKNOWN){}
    TransferTask():
            TransferTask(nullptr){}
    ~TransferTask(){
    }

    // whether has next package to send
    virtual bool has_next_package() = 0;

    virtual TransferRequest* get_next_package() = 0;

    // if failed & can not resume from breakpoint, reset the task and redo it
    virtual int reset() = 0;

    virtual void set_context(std::shared_ptr<TaskContext> _ctx){
        context = _ctx;
    }
    virtual std::shared_ptr<TaskContext> get_context(){
        return context;
    }

    bool operator<(TransferTask const& task2){
        return id < task2.id;
    }

    uint64_t get_id(){
        return id;
    }
    void set_id(const uint64_t& _id){
        id = _id;
    }

    TaskStatus get_status(){
        return status.load();
    }
    void set_status(const TaskStatus& _status){
        status.store(_status);
    }

    uint64_t get_ts(){
        return ts;
    }
    void set_ts(const uint64_t _ts){
        ts = _ts;
    }
};
#endif
