/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicate.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include "replicate.h"
#include "log/log.h"
#include "common/config_parser.h"
#include "rep_receiver.h"
#include "common/journal_meta_handle.hpp"
#include <grpc++/server_builder.h>
#include "dr_functions.hpp"
using std::string;
const system_clock::duration DEADLINE(std::chrono::duration<int>(10)); // TODO:

static bool cmp(std::shared_ptr<RepVolume> a,std::shared_ptr<RepVolume> b){
    system_clock::time_point now = system_clock::now();
    if(now - a->get_last_task_time() > DEADLINE || now - b->get_last_task_time() > DEADLINE)
        return a->get_last_task_time() > b->get_last_task_time(); // earlier has higher priority
    if(a->get_priority() != b->get_priority())
        return a->get_priority() < b->get_priority(); // bigger priority id has higher priority
    if(a->get_pending_journals_size() != b->get_pending_journals_size())// bigger size has higger priority
        return a->get_pending_journals_size() < b->get_pending_journals_size();
    return a->get_last_task_time() < b->get_last_task_time();
}

Replicate::Replicate(std::shared_ptr<CephS3Meta> meta,
        const string& mount, const string& addr,
        const std::shared_ptr<grpc::ChannelCredentials>& creds):
        meta_(meta),
        mount_path_(mount),
        running_(true),
        enable_(true),
        dispatch_pool_(new sg_threads::ThreadPool(DESPATCH_THREAD_CNT)){
    uuid_.append("uuid_replicator");// TODO:
    // init replicate client
    LOG_INFO << "start replicate client connect to " << addr;
    client_.reset(new RepClient(grpc::CreateChannel(
        addr, creds),MAX_REPLICATE_TASK));

    // start work thread which construct and distribute tasks
    work_thread_.reset(new std::thread(&Replicate::run,this));
    recycle_thread_.reset(new std::thread(&Replicate::recycle_tasks,this));
    marker_thread_.reset(new std::thread(&Replicate::sync_markers,this));
}

Replicate::~Replicate(){
    running_ = false;
    if(work_thread_ && work_thread_->joinable()){
        work_thread_->join();
    }
    if(recycle_thread_ && recycle_thread_->joinable()){
        recycle_thread_->join();
    }
    if(marker_thread_ && marker_thread_->joinable()){
        marker_thread_->join();
    }
}

int Replicate::add_volume(const string& vol_id){
    std::unique_lock<std::mutex> lck(volume_mtx_);
    auto it = volumes_.find(vol_id);
    if(it != volumes_.end()){
        LOG_WARN << "volume is already in replicator!";
        return -1;
    }
    lck.unlock();
    std::shared_ptr<RepVolume> vol(new RepVolume(uuid_,vol_id,mount_path_,meta_));
    LOG_INFO << "add volume " << vol_id;
    lck.lock();
    volumes_.insert(std::pair<std::string,std::shared_ptr<RepVolume>>(vol_id,vol));
    priority_list_.push_back(vol);
    return 0;
}

int Replicate::remove_volume(const string& vol_id){
    std::lock_guard<std::mutex> lck(volume_mtx_);
    auto it = volumes_.find(vol_id);
    if(it == volumes_.end()){
        LOG_WARN << "volume is not found in replicator!";
        return -1;
    }
    it->second->set_enable(false);
    it->second->set_delete(true);
    return 0;
}

void Replicate::task_end_callback(std::shared_ptr<RepTask> task){
    LOG_DEBUG << task->vol_id << " callback, task " << task->id << ","
        << task->info.key;
    queue_.push(task);
}

void Replicate::recycle_tasks(){
    while(running_){
        // TODO: to replace as a pop operation with timeout 
        std::shared_ptr<RepTask> task = queue_.pop();// blocking when queue is empty
        auto it = volumes_.find(task->vol_id);
        DR_ASSERT(it != volumes_.end());
        std::shared_ptr<RepVolume>& volume = it->second;
        bool ret = volume->recycle_task(task);
        if(ret){
            JournalMarker marker = volume->get_consuming_marker();
            markers_queue_.push(marker);
        }
    }
}

void Replicate::sync_markers(){
    while(running_){
        JournalMarker marker;
        if(markers_queue_.pop(marker)){
            string vol = dr_server::get_vol_by_key(marker.cur_journal());
            if(!client_->sync_marker(vol,marker)){
                LOG_ERROR << "sync marker failed," << marker.cur_journal();
            }
            else
                LOG_DEBUG << "syncing marker:" << marker.cur_journal();
        }
    }
}

void Replicate::wait_for_client_ready(){
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
    while((s = client_->get_state(false)) != CLIENT_READY && running_){
        LOG_INFO << "replicate client state:"
            << client_->get_printful_state(s);
        if(CLIENT_IDLE == s){
            s = client_->get_state(true);
            client_->wait_for_state_change(s,
                std::chrono::system_clock::now() + std::chrono::seconds(1));
        }
        else if(CLIENT_CONNECTING == s){
            client_->wait_for_state_change(s,
                std::chrono::system_clock::now() + std::chrono::seconds(10));
        }
        else{
            std::this_thread::sleep_for(std::chrono::seconds(5));
            client_->get_state(true);
        }
    };
}

void Replicate::dispatch(std::shared_ptr<RepVolume> rep_vol){
    std::function <void(Replicate*,std::shared_ptr<RepTask>)> c = &Replicate::task_end_callback;
    std::shared_ptr<RepTask> task = rep_vol->get_next_replicate_task();
    if(!task)
        return;
    task->callback = std::bind(c,this,std::placeholders::_1);
    client_->submit_task(task);
}

void Replicate::run(){
    wait_for_client_ready();
    while(running_){
        if(!enable_ || volumes_.empty()){
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        // sort volume by priority, and despatch tasks
        std::function<void(Replicate*,std::shared_ptr<RepVolume>)> f = &Replicate::dispatch;
        std::unique_lock<std::mutex> lck(volume_mtx_);
        priority_list_.sort(cmp);
        dispatch_pool_->submit(std::bind(f,this,priority_list_.front()));
        lck.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));// TODO: config
    }
}

