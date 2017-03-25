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
#include "sg_server/transfer/net_sender.h"
#include "task_handler.h"
using huawei::proto::REPLICATOR;

std::shared_ptr<TransferTask> ReplicatorContext::get_next_replicate_task(){
    std::lock_guard<std::mutex> lck(mtx_);
    // first check for failed task and redo it if window was full
    if(!task_window_.has_free_slot()){
        std::shared_ptr<TransferTask> task = task_window_.get_failed_task();
        if(task){
            task->set_status(T_UNKNOWN);
            //redo or resume from breakpoint?
            task->reset();
            LOG_WARN << "restart failed task:" << task->get_id();
        }
        return task;
    }
    // TODO:check timeout/failed task when window is not full used
    if(task_window_.get_failed_task()){
        std::shared_ptr<TransferTask> task = task_window_.get_failed_task();
        task->set_status(T_UNKNOWN);
        // redo or resume from breakpoint?
        task->reset();
        LOG_WARN << "restart failed task:" << task->get_id();
        return task;
    }
    // get consumable journals
    if(pending_journals_.size() < 1){
        std::list<JournalElement> list;
        int res = get_consumable_journals(temp_c_marker_,max_pending_tasks_,list);
        if(0 != res){
            LOG_ERROR <<  vol_ << ":get consumable_journals failed!";
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

    // if the journal was been transferring, wait for it's done;
    // donot transfer the same journal concurrently
    auto& last_task = task_window_.get_last_sent_task();
    if(nullptr != last_task && T_DONE != last_task->get_status()){
        std::shared_ptr<RepContext> ctx = 
            std::dynamic_pointer_cast<RepContext>(last_task->get_context());
        SG_ASSERT(ctx != nullptr);
        string last_journal = sg_util::construct_journal_key(vol_,ctx->get_j_counter());
        const string& journal = pending_journals_.front().journal();
        if(j_meta_mgr_->compare_journal_key(last_journal,journal) == 0){
            return nullptr;
        }
    }

    // get next journal task
    std::shared_ptr<TransferTask> task = construct_task(pending_journals_.front());
    if(task){
        task->set_id(++seq_id_);
        SG_ASSERT(true == task_window_.add_task(task));
        LOG_INFO << "construct journal task[" << task->get_id() << "],journal:"
            << pending_journals_.front().journal();

        pending_journals_.pop_front();
        return task;
    }
    else{
        LOG_ERROR << "construct task failed!";
        return nullptr;
    }
}

// recycle_task, return true if consuming marker was updated
void ReplicatorContext::recycle_task(std::shared_ptr<TransferTask>& t){
    LOG_DEBUG << vol_ << " recycle task:" << t->get_id();

    // 1. check whether the slide window move right
    std::lock_guard<std::mutex> lck(mtx_);
    if(!task_window_.ack_task(t))
        return;

    // 2. compare new consumer marker to last one
    std::shared_ptr<TransferTask>& task = task_window_.get_last_acked_task();
    std::shared_ptr<RepContext> ctx = 
            std::dynamic_pointer_cast<RepContext>(task->get_context());
    SG_ASSERT(ctx != nullptr);
    JournalMarker temp_marker;
    temp_marker.set_cur_journal(
        sg_util::construct_journal_key(vol_,ctx->get_j_counter()));
    temp_marker.set_pos(ctx->get_end_off());
    int result = j_meta_mgr_->compare_marker(temp_marker,c_marker_);
    if(result > 0){
        // 3. try to update new marker
        c_marker_.CopyFrom(temp_marker);
        TaskHandler::instance().add_marker_context(this,c_marker_);

        LOG_INFO << "try update " << ctx->get_vol_id()
            << " replicator consumer marker at "
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
    if(!initialized_ && 0!=init()) // consumer marker not initialized
        return false;
    if(task_window_.get_failed_task())
        return true;
    if(!task_window_.has_free_slot()) // requirement
        return false;
    JournalMarker p_marker;
    if(0 != get_producer_marker(p_marker)){
//        LOG_ERROR << vol_ << ":get producer marker failed!";
        return false;
    }
    if(task_window_.get_last_sent_task()){
        auto& task = task_window_.get_last_sent_task();
        std::shared_ptr<RepContext> ctx = 
            std::dynamic_pointer_cast<RepContext>(task->get_context());
        SG_ASSERT(ctx != nullptr);

        // compare journal marker of last sent task to producer marker
        string journal = sg_util::construct_journal_key(vol_,ctx->get_j_counter());
        JournalMarker temp_marker;
        temp_marker.set_cur_journal(journal);
        temp_marker.set_pos(ctx->get_end_off());
        int result = j_meta_mgr_->compare_marker(temp_marker,p_marker);
        if(result < 0)
            return true;
        SG_ASSERT(result <= 0);
    }
    else{
        if(j_meta_mgr_->compare_marker(p_marker,c_marker_) > 0)
            return true;
    }
    return false;
}

int ReplicatorContext::init(){
    int res = get_consumer_marker(c_marker_);
    if(res != 0)
        return -1;
    temp_c_marker_.CopyFrom(c_marker_);
    pending_journals_.clear();
    LOG_INFO << "init [" << vol_ << "] marker in replicator.";
    if(task_window_.clear() != 0){
        LOG_ERROR << "there is unfinished task in window!";
        return -1;
    }
    initialized_ = true;
    return 0;
}

int ReplicatorContext::cancel_all_tasks(){
    LOG_INFO << "cancel volume [" << vol_ << "]'s replicate tasks.";
    task_window_.cancel_all_tasks();
}

// construct JournalTask
std::shared_ptr<TransferTask> ReplicatorContext::construct_task(const JournalElement& e){
    JournalMeta meta;
    RESULT res = j_meta_mgr_->get_journal_meta(e.journal(),meta);
    SG_ASSERT(res == DRS_OK);
    uint64_t c;
    SG_ASSERT(true == sg_util::extract_major_counter_from_journal_key(e.journal(),c));
    auto f = std::bind(&ReplicatorContext::recycle_task,this,std::placeholders::_1);
    bool is_open = meta.status() == OPENED ? true:false;
    std::shared_ptr<RepContext> ctx(new RepContext(vol_,c,e.end_offset(),is_open,f));
    std::shared_ptr<TransferTask> task(
        new JournalTask(e.start_offset(),conf_.journal_mount_point + meta.path(), ctx));
    task->set_status(T_WAITING);
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

