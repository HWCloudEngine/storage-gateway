/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    markers_maintainer.h
* Author: 
* Date:         2017/02/06
* Version:      1.0
* Description:
* 
************************************************/
#ifndef MARKERS_MAINTAINER_H_
#define MARKERS_MAINTAINER_H_
#include "common/thread_pool.h"
#include "replicator_context.h"
#include "sg_server/transfer/net_sender.h"
class MarkersMaintainer{
private:
    bool running_;
    // input queue
    std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> in_que_;
    // threads to sync markers, with inner blocking_queue to hold markers
    std::unique_ptr<sg_threads::ThreadPool> tp_;
public:
    static MarkersMaintainer& instance(){
        static MarkersMaintainer t;
        return t;
    }

    MarkersMaintainer(MarkersMaintainer&) = delete;
    MarkersMaintainer& operator=(MarkersMaintainer const&) = delete;
    // add sync-marker function to thread_pools' inner queue
    void init(std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> in_que);

private:
    MarkersMaintainer():running_(true){}
    ~MarkersMaintainer(){
        running_ = false;
    }

    // thread work loop
    void work();
};
#endif
