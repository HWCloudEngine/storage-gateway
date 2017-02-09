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
#include "rep_receiver.h"
#include "common/journal_meta_handle.h"
#include <grpc++/server_builder.h>
#include "../dr_functions.h"
#include "replicator.h"
using std::string;
const uint64_t DEADLINE = 300; // TODO:

static bool cmp(std::shared_ptr<RepVolume> a,std::shared_ptr<RepVolume> b){
    uint64_t now = time(NULL);
    if(now - a->get_last_served_time() > DEADLINE || now - b->get_last_served_time() > DEADLINE)
        return a->get_last_served_time() > b->get_last_served_time(); // earlier has higher priority
    if(a->get_priority() != b->get_priority())
        return a->get_priority() < b->get_priority(); // bigger priority id has higher priority
    return a->get_last_served_time() < b->get_last_served_time();
}

RepScheduler::RepScheduler(std::shared_ptr<CephS3Meta> meta,
        const string& mount):
        meta_(meta),
        mount_path_(mount),
        running_(true),
        enable_(true),
        dispatch_pool_(new sg_threads::ThreadPool(DESPATCH_THREAD_CNT,DESPATCH_THREAD_CNT*2)){
    uuid_.append("uuid_replicator");// TODO:

    // start work thread which disapatch volume tasks
    work_thread_.reset(new std::thread(&RepScheduler::run,this));
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
    std::shared_ptr<RepVolume> vol(new RepVolume(vol_id,meta_));
    LOG_INFO << "add volume " << vol_id;
    std::shared_ptr<ReplicatorContext> reptr(
        new ReplicatorContext(vol_id,meta_,mount_path_));
    vol->register_replicator(reptr);
    lck.lock();
    volumes_.insert(std::pair<std::string,std::shared_ptr<RepVolume>>(vol_id,vol));
    return 0;
}

int RepScheduler::remove_volume(const string& vol_id){
    std::lock_guard<std::mutex> lck(volume_mtx_);
    auto it = volumes_.find(vol_id);
    if(it == volumes_.end()){
        LOG_WARN << "volume is not found in replicator!";
        return -1;
    }
    it->second->to_delete();
    volumes_.erase(it);
    return 0;
}

void RepScheduler::notify_rep_state_changed(const string& vol){
    // TODO:
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
    while((s = Transmitter::instance().get_state(false)) != CLIENT_READY && running_){
        LOG_INFO << "replicate client state:"
            << Transmitter::instance().get_printful_state(s);
        if(CLIENT_IDLE == s){
            s = Transmitter::instance().get_state(true);
            Transmitter::instance().wait_for_state_change(s,
                std::chrono::system_clock::now() + std::chrono::seconds(1));
        }
        else if(CLIENT_CONNECTING == s){
            Transmitter::instance().wait_for_state_change(s,
                std::chrono::system_clock::now() + std::chrono::seconds(10));
        }
        else{
            std::this_thread::sleep_for(std::chrono::seconds(5));
            Transmitter::instance().get_state(true);
        }
    };
}

void RepScheduler::dispatch(std::shared_ptr<RepVolume> rep_vol){
    rep_vol->set_task_generating_flag(true);
    rep_vol->serve();
    rep_vol->set_task_generating_flag(false);
}

void RepScheduler::run(){
    wait_for_client_ready();
    while(running_){
        if(!enable_ || volumes_.empty()){
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        // sort volume by priority, and dispatch tasks
        auto f = &RepScheduler::dispatch;
        std::unique_lock<std::mutex> lck(volume_mtx_);
        for(auto it=volumes_.begin();it!=volumes_.end();++it){
            auto& vol = it->second;
            if(!vol->get_task_generating_flag() && vol->need_replicate())
                priority_list_.push_back(vol);
        }
        lck.unlock();
        priority_list_.sort(cmp);
        for(auto& vol:priority_list_){
            dispatch_pool_->submit(std::bind(f,this,vol));
        }
        priority_list_.clear();
    }
}

