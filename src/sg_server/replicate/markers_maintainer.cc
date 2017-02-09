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
#include "../dr_functions.h"
#include "rep_transmitter.h"
int MarkersMTR::add_marker_to_sync(ReplicatorContext* rep_ctx){
    // TODO:
    auto f = std::bind(&MarkersMTR::do_sync,this,rep_ctx);
    if(thread_pool_->submit(f))
        return 0;
    else
        return -1;
}

void MarkersMTR::do_sync(ReplicatorContext* rep_ctx){
    if(rep_ctx){
        LOG_WARN << "ReplicatorContext is null when try to sync markers";
        return;
    }
    // sync operation maybe waste if the same volume has more than one copy in the queue
    string vol = rep_ctx->get_vol_id();
    JournalMarker marker = rep_ctx->get_transferring_marker();
    bool ret = Transmitter::instance().sync_marker(vol,marker);
    if(!ret){
        LOG_ERROR << "sync remote producer marker failed:" << marker.cur_journal();
        return;
    }
    // sync local volume consumer marker
    int up_ret = rep_ctx->update_consumer_marker(marker);
    if(up_ret){
        LOG_ERROR << "update [" << vol << "] consumer marker failed!";
    }
}

