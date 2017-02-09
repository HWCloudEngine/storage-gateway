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
#include "replicator.h"
class MarkersMTR{
public:
    static MarkersMTR& instance(){
        static MarkersMTR t;
        return t;
    }

    MarkersMTR(MarkersMTR&) = delete;
    MarkersMTR& operator=(MarkersMTR const&) = delete;
    // add sync-marker function to thread_pools' inner queue
    int add_marker_to_sync(ReplicatorContext* rep_ctx);

private:
    MarkersMTR():
        thread_pool_(new sg_threads::ThreadPool()){
    }

    ~MarkersMTR(){}

    // volumes whose marker need sync
    void do_sync(ReplicatorContext* rep_ctx);

    // threads to sync markers, with inner blocking_queue to hold markers
    std::unique_ptr<sg_threads::ThreadPool> thread_pool_;
};
#endif
