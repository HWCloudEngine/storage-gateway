/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_task_generator.cc
* Author: 
* Date:         2017/02/14
* Version:      1.0
* Description:
* 
************************************************/
#include "rep_task_generator.h"
TaskGenerator::TaskGenerator(BlockingQueue<std::shared_ptr<RepVolume>>& in,
        std::shared_ptr<BlockingQueue<std::shared_ptr<RepTask>>> out):
        running_(true),
        vol_queue_(in),
        task_queue_(out),
        tp_(new sg_threads::ThreadPool(
            DESPATCH_THREAD_CNT,DESPATCH_THREAD_CNT)){
    // start all thread in tp,each work use a thread exclusively
    for(int i=0; i<DESPATCH_THREAD_CNT; i++){
        tp_->submit(std::bind(&TaskGenerator::work,this));
    }
}

TaskGenerator::~TaskGenerator(){
    running_ = false;
}

void TaskGenerator::work(){
    while(running_){
        std::shared_ptr<RepVolume> rep_vol = vol_queue_.pop();
        rep_vol->set_task_generating_flag(true);
        while(true){
            std::shared_ptr<RepTask> task = rep_vol->get_next_task();
            if(task == nullptr)
                break;
            task_queue_->push(task);
        }
        rep_vol->set_task_generating_flag(false);
    }
}
