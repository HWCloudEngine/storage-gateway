/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_scheduler.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include <time.h> // time,time_t
#include <chrono>
#include "rep_scheduler.h"
#include "log/log.h"
#include "common/config_parser.h"
#include "../sg_util.h"
#include "replicator_context.h"
#include "sg_server/transfer/net_sender.h"
using std::string;

RepScheduler::RepScheduler(std::shared_ptr<CephS3Meta> meta,
        Configure& conf,
        BlockingQueue<std::shared_ptr<RepVolume>>& vol_queue):
        meta_(meta),
        conf_(conf),
        running_(true),
        enable_(true),
        vol_queue_(vol_queue){

    // start work thread which disapatch volume tasks
    work_thread_.reset(new std::thread(&RepScheduler::work,this));
}

RepScheduler::~RepScheduler(){
    running_ = false;
    if(work_thread_ && work_thread_->joinable()){
        work_thread_->join();
    }
}

int RepScheduler::add_volume(const string& vol_id){
    std::unique_lock<std::mutex> lck(volume_mtx_);
    auto it = volumes_.find(vol_id);
    if(it != volumes_.end()){
        LOG_WARN << "volume is already in replicator!";
        return -1;
    }
    lck.unlock();
    std::shared_ptr<RepVolume> vol(new RepVolume(vol_id,conf_,
                meta_/*VolumeMetaManager*/,meta_/*JournalMetaManager*/));
    LOG_INFO << "add volume " << vol_id;
    std::shared_ptr<ReplicatorContext> reptr(
        new ReplicatorContext(vol_id,meta_,conf_));
    vol->register_replicator(reptr);
    vol->recover_replication();
    lck.lock();
    volumes_.insert(std::pair<std::string,std::shared_ptr<RepVolume>>(vol_id,vol));
    return 0;
}

int RepScheduler::remove_volume(const string& vol_id){
    std::lock_guard<std::mutex> lck(volume_mtx_);
    auto it = volumes_.find(vol_id);
    if(it == volumes_.end()){
        LOG_WARN << "volume[" << vol_id << "] is not found when remove!";
        return -1;
    }
    it->second->delete_rep_volume();
    volumes_.erase(it);
    return 0;
}

void RepScheduler::notify_rep_state_changed(const string& vol_id){
    std::lock_guard<std::mutex> lck(volume_mtx_);
    auto it = volumes_.find(vol_id);
    if(it == volumes_.end()){
        LOG_WARN << "volume[" << vol_id << "] is not found when notify!";
        return;
    }
    it->second->notify_rep_state_changed();
    return;
}

void RepScheduler::wait_for_client_ready(){
    //rpc channel state
    /*
    CONNECTING: The channel is trying to establish a connection and is waiting to make progress 
        on one of the steps involved in name resolution, TCP connection establishment or TLS handshake.
        This may be used as the initial state for channels upon creation.

    READY: The channel has successfully established a connection all the way through TLS handshake (or equivalent)
        and all subsequent attempt to communicate have succeeded (or are pending without any known failure ).

    TRANSIENT_FAILURE: There has been some transient failure. 

    IDLE: This is the state where the channel is not even trying to create a connection
        because of a lack of new or pending RPCs. New RPCs MAY be created in this state. 
        Any attempt to start an RPC on the channel will push the channel out of this state to connecting.
        When there has been no RPC activity on a channel for a specified IDLE_TIMEOUT, i.e.,
        no new or pending (active) RPCs for this period, channels that are READY or CONNECTING switch to IDLE. 

    SHUTDOWN: This channel has started shutting down.
    ref: https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md
    */
    ClientState s;
    while((s = NetSender::instance().get_state(false)) != CLIENT_READY && running_){
        LOG_INFO << "replicate client state:"
            << NetSender::instance().get_printful_state(s);
        if(CLIENT_IDLE == s){
            s = NetSender::instance().get_state(true);
            NetSender::instance().wait_for_state_change(s,
                std::chrono::system_clock::now() + std::chrono::seconds(1));
        }
        else if(CLIENT_CONNECTING == s){
            NetSender::instance().wait_for_state_change(s,
                std::chrono::system_clock::now() + std::chrono::seconds(10));
        }
        else{
            std::this_thread::sleep_for(std::chrono::seconds(5));
            NetSender::instance().get_state(true);
        }
    };
}

void RepScheduler::work(){
    wait_for_client_ready();
    while(running_){
        if(volumes_.empty()){
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        // wait for any volume producer marker changed or timeout,
        // for not all journals could be replicated at one loop time,
        // RepScheduler will check the that when timeout
        meta_->wait_for_replicator_producer_maker_changed(1000);

        // sort volume by priority, and dispatch tasks
        std::unique_lock<std::mutex> lck(volume_mtx_);
        for(auto it=volumes_.begin();it!=volumes_.end();++it){
            auto& vol = it->second;
            if(!vol->get_task_generating_flag() && vol->need_replicate())
                priority_list_.push_back(vol);
        }
        lck.unlock();
        priority_list_.sort();
        for(auto vol:priority_list_){
            vol_queue_.push(vol);
        }
        priority_list_.clear();
    }
}

