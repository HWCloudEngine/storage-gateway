/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    markers_maintainer.cc
* Author: 
* Date:         2017/02/06
* Version:      1.0
* Description:
* 
************************************************/
#include "markers_maintainer.h"
#include "rep_transmitter.h"
#include "../sg_util.h"
#include "rep_transmitter.h"
#define MARKER_MAINTAINER_THREAD_COUNT (2)

void MarkersMaintainer::init(
        std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> in_que){
    in_que_ = in_que;
    tp_.reset(new sg_threads::ThreadPool(MARKER_MAINTAINER_THREAD_COUNT,
                MARKER_MAINTAINER_THREAD_COUNT));
    // start all thread in tp, run work methon in concurently
    for(int i=0; i<MARKER_MAINTAINER_THREAD_COUNT; i++){
        tp_->submit(std::bind(&MarkersMaintainer::work,this));
    }
}

void MarkersMaintainer::work(){
    while(running_){
        std::shared_ptr<MarkerContext> ctx = in_que_->pop();
        DR_ASSERT(ctx != nullptr);
        if(nullptr == ctx->rep_ctx){
            LOG_WARN << "ReplicatorContext is null when try to sync markers";
            return;
        }
        // sync operation maybe waste if the same volume has more than one copy in the queue
        string vol = ctx->rep_ctx->get_vol_id();
        bool ret = Transmitter::instance().sync_marker(vol,ctx->marker);
        if(!ret){
            LOG_ERROR << "sync remote producer marker failed:"
                << ctx->marker.cur_journal();
            return;
        }
        // sync local volume consumer marker
        int up_ret = ctx->rep_ctx->update_consumer_marker(ctx->marker);
        if(up_ret){
            LOG_ERROR << "update [" << vol << "] consumer marker failed:"
                << ctx->marker.cur_journal();
        }
    }
}

