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
            task->set_status(T_UNKNOWN);
        return task;
    }
    // TODO:check timeout/failed task when window is not full used
    if(task_window_.get_failed_task()){
        std::shared_ptr<RepTask> task = task_window_.get_failed_task();
        task->set_status(T_UNKNOWN);
        return task;
    }
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
        task->set_id(++seq_id_);
        task->init();
        task_window_.add_task(task);
        pending_journals_.pop_front();
        return task;
    }
    else{
        LOG_ERROR << "construct task failed!";
        return nullptr;
    }
}

// recycle_task, return true if consuming marker was updated
void ReplicatorContext::recycle_task(std::shared_ptr<RepTask>& t){
    LOG_DEBUG << vol_ << " recycle task:" << t->get_id();

    // 1. check whether the slide window move right
    std::lock_guard<std::mutex> lck(mtx_);
    if(!task_window_.ack_task(t))
        return;

    // 2. compare new consumer marker to last one
    std::shared_ptr<RepTask>& task = task_window_.get_last_acked_task();
    JournalMarker temp_marker;
    temp_marker.set_cur_journal(
        sg_util::construct_journal_key(vol_,task->get_j_counter()));
    temp_marker.set_pos(task->get_end_off());
    int result = sg_util::marker_compare(temp_marker,c_marker_);
    if(result > 0){
        // 3. try to update new marker
        c_marker_.CopyFrom(temp_marker);
        Transmitter::instance().add_marker_context(this,c_marker_);
        
        LOG_INFO << "to update " << task->get_vol_id() << " replicator consuming marker at "
            << temp_marker.cur_journal() << ":" << temp_marker.pos();
        return;
    }
    else if(result == 0){
        LOG_DEBUG << "new consuming marker the same as the last one:"
            << temp_marker.cur_journal() << ":" << temp_marker.pos() << "=="
            << c_marker_.cur_journal() << ":" << c_marker_.pos();
    }
    else{
        LOG_ERROR << "new consuming marker ahead of the last one:"
            << temp_marker.cur_journal() << ":" << temp_marker.pos() << "<"
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
        auto& task = task_window_.get_last_sent_task();
        int64_t marker_counter;
        DR_ASSERT(true == sg_util::extract_counter_from_object_key(
                p_marker.cur_journal(),marker_counter));
        // compare journal marker of completed task to producer marker
        if(marker_counter > task->get_j_counter())
            return true;
        else if(marker_counter == task->get_j_counter()){
            if(p_marker.pos() > task->get_end_off())
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
    JournalMeta meta;
    RESULT res = j_meta_mgr_->get_journal_meta(e.journal(),meta);
    DR_ASSERT(res == DRS_OK);
    std::shared_ptr<RepTask> task(
        new JournalTask(e.start_offset(),mount_path_ + meta.path()));
    task->set_vol_id(vol_);
    task->set_status(T_WAITING);
    int64_t c;
    DR_ASSERT(true == sg_util::extract_counter_from_object_key(e.journal(),c));
    task->set_j_counter(c);
    task->set_end_off(e.end_offset());
    task->set_callback(std::bind(&ReplicatorContext::recycle_task,this,std::placeholders::_1));
    task->set_is_opened(meta.status() == huawei::proto::OPENED? true:false);
    task->set_block_size(TASK_BLOCK_SIZE);
    task->set_ts(time(nullptr));
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

