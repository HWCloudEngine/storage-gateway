/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicator.cc
* Author: 
* Date:         2017/01/17
* Version:      1.0
* Description:
* 
************************************************/
#include "replicator_context.h"
#include "log/log.h"
#include "../sg_util.h"
#include <time.h> // time,time_t
#include <grpc++/grpc++.h>
#include "rep_transmitter.h"
using huawei::proto::REPLICATOR;

std::shared_ptr<RepTask> ReplicatorContext::get_next_replicate_task(){
    std::lock_guard<std::mutex> lck(mtx_);
    // first check for failed task and redo it if window was full
    if(!task_window_.has_free_slot()){
        std::shared_ptr<RepTask> task = task_window_.get_failed_task();
        if(task)
            task->status = T_UNKNOWN;
        return task;
    }
    // TODO:check timeout/failed task when window is not full used
    if(task_window_.get_failed_task()){
        std::shared_ptr<RepTask> task = task_window_.get_failed_task();
        task->status = T_UNKNOWN;
        return task;
    }
    /*
    if(nullptr == task_window_.get_last_sent_task()){//no task sent, check the consuming marker
        if(!init_markers()){
            LOG_WARN << vol_ << ":init consumer marker failed!";
            return nullptr;
        }
    }
    */
    if(pending_journals_.size() < 1){
        std::list<JournalElement> list;
        int res = get_consumable_journals(temp_c_marker_,max_pending_tasks_,list);
        if(0 != res){
            LOG_ERROR <<  vol_ << ":get consumbale_journals failed!";
            return nullptr;
        }
        for(JournalElement& s:list){
            pending_journals_.push_back(std::move(s));
        }
        if(!pending_journals_.empty()){
            JournalElement& e = pending_journals_.back();
            temp_c_marker_.set_cur_journal(e.journal());
            temp_c_marker_.set_pos(e.end_offset());
        }
    }
    if(pending_journals_.empty())
        return nullptr;
    std::shared_ptr<RepTask> task = construct_task(pending_journals_.front());
    if(task){
        task->id = ++seq_id_;
        task_window_.add_task(task);
        pending_journals_.pop_front();
    }
    return task;
}

// recycle_task, return true if consuming marker was updated
void ReplicatorContext::recycle_task(std::shared_ptr<RepTask>& t){
    LOG_DEBUG << vol_ << " recycle task:" << t->id;
    std::lock_guard<std::mutex> lck(mtx_);
    if(!task_window_.ack_task(t))
        return;
    std::shared_ptr<RepTask>& task = task_window_.get_last_acked_task();
    if(task->info.key.compare(c_marker_.cur_journal()) > 0
        || (task->info.key.compare(c_marker_.cur_journal()) == 0 
            && task->info.end > c_marker_.pos())){
        c_marker_.set_cur_journal(task->info.key);
        c_marker_.set_pos(task->info.end);

        Transmitter::instance().add_marker_context(this,c_marker_);
        
        LOG_INFO << "to update " << task->vol_id << " replicator consuming marker at "
            << task->info.key << ":" << task->info.end;
        return;
    }
    else if(task->info.key.compare(c_marker_.cur_journal()) == 0 
            && task->info.end == c_marker_.pos()){
        LOG_DEBUG << "new consuming marker the same as the last one:"
            << task->info.key << ":" << task->info.end << "=="
            << c_marker_.cur_journal() << ":" << c_marker_.pos();
    }
    else{
        LOG_ERROR << "new consuming marker ahead of the last one:"
            << task->info.key << ":" << task->info.end << "<"
            << c_marker_.cur_journal() << ":" << c_marker_.pos();
        DR_ERROR_OCCURED();
    }
    return;
}

bool ReplicatorContext::has_journals_to_transfer(){
    std::lock_guard<std::mutex> lck(mtx_);
    if(task_window_.get_failed_task())
        return true;
    if(!task_window_.has_free_slot()) // requirement
        return false;
    JournalMarker p_marker;
    if(0 != get_producer_marker(p_marker)){
        LOG_ERROR << vol_ << ":get producer marker failed!";
        return false;
    }
    if(task_window_.get_last_sent_task()){
        auto& info = task_window_.get_last_sent_task()->info;
        int ret = p_marker.cur_journal().compare(info.key);
        if(ret > 0)
            return true;
        else if(ret == 0){
            if(p_marker.pos() > info.end)
                return true;
        }
        else {
            DR_ERROR_OCCURED();
        }
    }
    else{
        if(sg_util::marker_compare(p_marker,c_marker_) > 0)
            return true;
    }
    return false;
}

const JournalMarker& ReplicatorContext::get_transferring_marker(){
    return c_marker_;
}

bool ReplicatorContext::init_markers(){
    int res = get_consumer_marker(c_marker_);
    if(res != 0)
        return false;
    temp_c_marker_.CopyFrom(c_marker_);
    pending_journals_.clear();
    LOG_INFO << "init [" << vol_ << "] marker in replicator.";
    return true;
}

std::shared_ptr<RepTask> ReplicatorContext::construct_task(const JournalElement& e){
    std::shared_ptr<RepTask> task(new RepTask);
    task->vol_id = vol_;
    task->status = T_WAITING;
    task->info.key = e.journal();
    task->info.pos = e.start_offset();
    task->info.end = e.end_offset();
    task->callback = std::bind(&ReplicatorContext::recycle_task,this,std::placeholders::_1);
    JournalMeta meta;
    RESULT res = j_meta_mgr_->get_journal_meta(e.journal(),meta);
    DR_ASSERT(res == DRS_OK);
    task->info.path = mount_path_ + meta.path();
    task->info.is_opened = meta.status() == huawei::proto::OPENED? true:false;
    task->tp = time(nullptr);
    return task;
}

CONSUMER_TYPE ReplicatorContext::get_type(){
    return REPLICATOR;
}

int ReplicatorContext::get_consumer_marker(JournalMarker& marker){
    if(DRS_OK == j_meta_mgr_->get_consumer_marker(vol_,REPLICATOR,marker))
        return 0;
    else
        return -1;
}

int ReplicatorContext::update_consumer_marker(const JournalMarker& marker){
    if(DRS_OK == j_meta_mgr_->update_consumer_marker(vol_,REPLICATOR,marker))
        return 0;
    else
        return -1;
}

int ReplicatorContext::get_consumable_journals(
        const JournalMarker& marker,
        const int limit, std::list<JournalElement>& list){
    if(DRS_OK == j_meta_mgr_->get_consumable_journals(vol_,marker,limit,list,REPLICATOR))
        return 0;
    else
        return -1;
}

int ReplicatorContext::get_producer_marker(JournalMarker& marker){
    if(DRS_OK == j_meta_mgr_->get_producer_marker(vol_,REPLICATOR,marker))
        return 0;
    else
        return -1;
}

