/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    dr_pipeline.hpp
* Author: 
* Date:         2016/11/15
* Version:      1.0
* Description:
* 
************************************************/
#ifndef DR_PIPELINE_HPP_
#define DR_PIPELINE_HPP_
#include <atomic>
#include <vector>
#include <mutex>
#include <cstdint>
#include "common/blocking_queue.h"
namespace dr_pipeline{
template<typename Task>
class StageBase{
public:
    StageBase():done_(true){}
    ~StageBase(){}
    void prepare(){}
    void end(){}
    virtual void process()=0;
    void run(){
        prepare();
        while(!done_){
            process();
        }
        end();
    }
    bool get_done(){
        return done_.load();
    }
    bool set_done(bool done){
        return done_.exchange(done);
    }
private:
    std::atomic<bool> done_;
    std::shared_ptr<BlockingQueue<std::shared_ptr<Task>>> in_que_;
    std::shared_ptr<BlockingQueue<std::shared_ptr<Task>>> out_que_;
};

template<typename Task>
class PipeLine{
public:
    PipeLine(){}
    ~PipeLine(){
        stop();
    }
    int add_task(std::shared_ptr<Task> t){
        return 0;
    }
    void add_stage(std::shared_ptr<StageBase> stage){

    }
    void start(){

    }
    void stop(){
    }
private:
    void run_acker(){
        while(running_){
            
        }
    }

    std::vector<std::shared_ptr<StageBase>> stages_;
    std::vector<std::shared_ptr<BlockingQueue<std::shared_ptr<Task>>>> queues_;
    std::vector<std::shared_ptr<sg_threads::ThreadPool>> thread_pools_;
    int id_;
    std::unique_ptr<std::thread> acker_;
//    BlockingQueue<std::shared_ptr<Task>> ack_que_;
    std::map<int,uint64_t> task_results_;
    std::atomic<bool> running_;
};
};
#endif
