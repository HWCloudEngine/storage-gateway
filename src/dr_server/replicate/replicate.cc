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
using std::string;
#define MAX_JOURNALS_PER_TASK 10
#define MAX_JOURNAL_SIZE (1024*1024*64) // TODO:read from config
#define MAX_REPLICATE_TASK  16 // TODO: read from config
using huawei::proto::JournalMeta;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::OPENED;
using huawei::proto::SEALED;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATER;

Replicate::Replicate(std::shared_ptr<CephS3Meta> meta,
        const string& mount, const string& addr,
        const std::shared_ptr<grpc::ChannelCredentials>& creds):
        meta_(meta),
        mount_path_(mount),
        running_(true),
        enable_(true){
    uuid_.append("uuid_replicator");// TODO:
    // init replicate client
    LOG_INFO << "start replicate client connect to " << addr;
    client_.reset(new RepClient(grpc::CreateChannel(
        addr, creds),MAX_REPLICATE_TASK));
    
   // start work thread which construct and distribute tasks
    work_thread_.reset(new std::thread(&Replicate::run,this));
    recycle_thread_.reset(new std::thread(&Replicate::recycle_tasks,this));
}

Replicate::~Replicate(){
    running_ = false;
    if(work_thread_ && work_thread_->joinable()){
        work_thread_->join();
    }
    if(recycle_thread_ && recycle_thread_->joinable()){
        recycle_thread_->join();
    }
}

int Replicate::add_volume(const string& vol_id){
    std::unique_lock<std::mutex> lck(volume_mtx_);
    auto it = volume_states_.find(vol_id);
    if(it != volume_states_.end()){
        LOG_WARN << "volume is already in replicator!";
        return -1;
    }
    lck.unlock();
    std::shared_ptr<ReplicatingState> state(new ReplicatingState());
    RESULT res = meta_->get_journal_marker(uuid_,vol_id,REPLICATER,
        &state->temp_consuming_marker);
    if(res != DRS_OK){
        LOG_ERROR << "add volume in replicator failed!";
        return -1;
    }
    LOG_INFO << "add volume " << vol_id;
    state->max_consuming_marker_in_process.CopyFrom(state->temp_consuming_marker);
    state->deleting = false;
    state->replicate_enable = true;
    state->delay = 0;
    lck.lock();
    volume_states_.insert(std::pair<std::string,std::shared_ptr<ReplicatingState>>(vol_id,state));
    return 0;
}

int Replicate::remove_volume(const string& vol_id){
    std::lock_guard<std::mutex> lck(volume_mtx_);
    auto it = volume_states_.find(vol_id);
    if(it == volume_states_.end()){
        LOG_WARN << "volume is not found in replicator!";
        return -1;
    }
    it->second->replicate_enable = false;
    it->second->deleting = true;
    return 0;
}

std::shared_ptr<RepTask> Replicate::construct_raw_task(const string& vol_id,
        std::shared_ptr<ReplicatingState>& state, int& count){
    std::shared_ptr<RepTask> task(new RepTask());
    task->vol_id = vol_id;
    task->priority = 0;
    task->status = T_WAITING;
    task->journals.reset(new std::vector<JournalInfo>());
    // add marked journal into task if the journal is not SEALED in last task
    if(state->max_consuming_marker_in_process.pos() < MAX_JOURNAL_SIZE){
        JournalInfo info;
        info.end = MAX_JOURNAL_SIZE; // TODO: sync producing marker if exsit
        info.pos = state->max_consuming_marker_in_process.pos();
        info.key =  state->max_consuming_marker_in_process.cur_journal();
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance().get_journal_meta(info.key,meta);
        DR_ASSERT(res == DRS_OK);
        if(meta.status() == OPENED)
            info.is_opened = true;
        else
            info.is_opened = false;
        info.path = mount_path_ + meta.path();
        task->journals->push_back(info);
        // since journal writer might prefetch a few journals, here if we meet
        // an opened journal, we should break, or we may miss this opened journals
        // for mistaking the cosumming marker
        if(info.is_opened){
            count = 0; // not consuming any journal in the pending list
            return task;
        }
    }
        
    int i=0;
    for(auto it=state->pending_journals_for_consuming.begin();i<count;i++,it++){
        JournalInfo info;
        info.pos = 0;
        info.key = *it;
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance().get_journal_meta(*it,meta);
        DR_ASSERT(res == DRS_OK);
        info.end = MAX_JOURNAL_SIZE; // TODO: sync producing marker if exsit
        if(meta.status() == OPENED)
            info.is_opened = true;
        info.path = mount_path_ + meta.path();
        task->journals->push_back(info);
        // since journal writer might prefetch a few journals, here if we meet
        // an opened journal, we should break, or we may miss this opened journals
        // for mistaking the cosumming marker
        if(info.is_opened){
            count = i+1;
            break;
        }
    }
    return task;
}

void Replicate::task_end_callback(std::shared_ptr<RepTask> task){
    LOG_DEBUG << task->vol_id << " task call back " << task->id;
    queue_.push(task);
}

void Replicate::recycle_tasks(){
    while(running_){
        // TODO: to replace as a pop operation with timeout 
        std::shared_ptr<RepTask> task = queue_.pop();// blocking when queue is empty
        auto it = volume_states_.find(task->vol_id);
        DR_ASSERT(it != volume_states_.end());
        std::shared_ptr<ReplicatingState>& state = it->second;
        // TODO: sort the tasks by first journal counter in each task
        bool flag = false;
        int i=0;
        std::lock_guard<std::mutex> lock(state->mtx);
        for(;i<state->tasks.size();++i){
            if(state->tasks[i]->status != T_DONE){
                // TODO: retry failed tasks??
                break;
            }
            flag = true;
        }
        if(!flag)
            continue;
        // update consuming marker
        JournalInfo& info = state->tasks[i-1]->journals->back();
        if(info.key.compare(state->temp_consuming_marker.cur_journal()) < 0){
            LOG_WARN << "new consuming marker is even less than the last one:"
                << info.key << "<" << state->temp_consuming_marker.cur_journal();
            continue;
        }
        state->temp_consuming_marker.set_cur_journal(info.key);
        state->temp_consuming_marker.set_pos(info.end);
        RESULT res = meta_->update_journals_marker(uuid_,task->vol_id,
            REPLICATER,state->temp_consuming_marker);
        if(res == DRS_OK){
            // remove finished tasks
            LOG_INFO << "update " << task->vol_id << " replicator consuming marker at "
                << info.key << ":" << info.end;
            state->tasks.erase(state->tasks.begin(),state->tasks.begin()+i);
        }
        else{
            LOG_WARN << "update marker failed" << info.key;
        }
    }
}

void Replicate::run(){
    std::function <void(Replicate*,std::shared_ptr<RepTask>)> c = &Replicate::task_end_callback;
    std::function <void(std::shared_ptr<RepTask>)> callback = 
        std::bind(c,this,std::placeholders::_1);
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
    do{
        ClientState s = client_->get_state(true);
        LOG_INFO << "replicate client state1:"
            << client_->get_printful_state(s);
        client_->wait_for_state_change(s,
            std::chrono::system_clock::now() + std::chrono::seconds(1));
        client_->wait_for_state_change(CLIENT_CONNECTING,
            std::chrono::system_clock::now() + std::chrono::seconds(10));
        s = client_->get_state(false);
        LOG_INFO << "replicate client state2:" 
            << client_->get_printful_state(s);
        if(CLIENT_FAILURE == s){
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        else if(CLIENT_SHUTDOWN == s){
            return;
        }
    }while(CLIENT_READY != client_->get_state(false) && running_);
    while(running_){
        if(!enable_){
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        // get every volume's journals, and construct replicate tasks
        for(auto it=volume_states_.begin();it!=volume_states_.end();++it){
            const string& vol_id = it->first;
            std::shared_ptr<ReplicatingState>& state = it->second;
            if(state->delay > 0){
                state->delay--;
                continue;
            }
            if(state->pending_journals_for_consuming.empty()){
                RESULT res = meta_->get_consumable_journals(uuid_,vol_id,
                    state->max_consuming_marker_in_process,
                    MAX_JOURNALS_PER_TASK,state->pending_journals_for_consuming);
                if(res != DRS_OK || state->pending_journals_for_consuming.empty()){
                    if(res != DRS_OK)
                        LOG_WARN << "get " << vol_id << " consumable journals failed!";
                    continue;
                }
            }
            int count = state->pending_journals_for_consuming.size() > MAX_JOURNALS_PER_TASK ?
                MAX_JOURNALS_PER_TASK : state->pending_journals_for_consuming.size();
            std::shared_ptr<RepTask> task = construct_raw_task(vol_id,state,count);
            DR_ASSERT(task != nullptr);
            if(client_->submit_task(task,callback) < 0){
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                task.reset(); // TODO:move to pending list and redo next loop
                continue;
            }
            else{
                // save task in vector in order?
                std::unique_lock<std::mutex> lock(state->mtx);
                state->tasks.push_back(task);
                lock.unlock();
                // update handling journals's consuming marker
                if(count == 0){
                    state->delay = 2000; // wait a while for no new journal was written
                    continue;
                }
                count = task->journals->size() < count ? task->journals->size():count;
                auto it = state->pending_journals_for_consuming.begin();
                auto it2 = it;
                std::advance(it2,count-1);
                JournalMeta meta;
                RESULT res = JournalMetaHandle::instance().get_journal_meta(*it2,meta);
                DR_ASSERT(res == DRS_OK);
                if(meta.status() == SEALED){
                    state->max_consuming_marker_in_process.set_pos(MAX_JOURNAL_SIZE);
                }
                else{
                    state->max_consuming_marker_in_process.set_pos(0); // TODO:
                }
                state->max_consuming_marker_in_process.set_cur_journal(*it2);
                LOG_DEBUG << "forward consuming marker: " << *it2;
                // remove handled journals
                it2++;
                state->pending_journals_for_consuming.erase(it,it2);
            }            
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
    }
}

