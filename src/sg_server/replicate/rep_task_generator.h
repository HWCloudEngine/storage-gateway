/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_task_generator.h
* Author: 
* Date:         2017/02/14
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_TASK_GENERATOR_H_
#define REP_TASK_GENERATOR_H_
#include "rep_type.h"
#include "common/thread_pool.h"
#include "rep_volume.h"
class TaskGenerator{
    // input queue
    BlockingQueue<std::shared_ptr<RepVolume>>& vol_queue_;
    // output queue
    std::shared_ptr<BlockingQueue<std::shared_ptr<RepTask>>> task_queue_;
    std::unique_ptr<sg_threads::ThreadPool> tp_;
    bool running_;
public:
    TaskGenerator(BlockingQueue<std::shared_ptr<RepVolume>>& in,
            std::shared_ptr<BlockingQueue<std::shared_ptr<RepTask>>> out);
    ~TaskGenerator();
private:
    void work();
};
#endif