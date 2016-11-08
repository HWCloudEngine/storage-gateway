/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_volume.hpp
* Author: 
* Date:         2016/11/15
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_VOLUME_HPP_
#define REP_VOLUME_HPP_
#include <vector>
#include <mutex>
#include <chrono>
#include <list>
#include <fstream>
#include "rep_type.h"
#include "common/journal_meta_handle.hpp"
#include "log/log.h"
#include "rpc/consumer.pb.h"
using huawei::proto::JournalMarker;
using huawei::proto::REPLICATER;
using std::chrono::system_clock;

class RepVolume{
public:
    RepVolume(std::string uuid,std::string vol_id,std::string mount,
            std::shared_ptr<CephS3Meta> meta):
            uuid_(uuid),
            vol_id_(vol_id),
            mount_path_(mount),
            meta_(meta),
            max_pending_tasks_(MAX_TASK_PER_VOL),
            enable_(true),
            seq_id_(0L){
        init_markers();
    }
    ~RepVolume(){}
    std::shared_ptr<RepTask> get_next_replicate_task(){
        std::lock_guard<std::mutex> lck(mtx_);
        // first check for failed task and redo it
        // TODO: sort tasks
        for(int i=0;i<tasks_.size();i++){
            if(tasks_[i]->status == T_ERROR){
                tasks_[i]->status = T_WAITING;
//                tasks_[i]->tp = system_clock.now();
                return tasks_[i];
            }
        }
        if(!enable_ || tasks_.size() >= max_pending_tasks_)
            return nullptr;
        if(!pre_info){
            if(!init_markers())
                return nullptr;
        }
        
        if(pre_info->is_opened){
        // if last replicating journal is opened, this journal may be appended writes
        // since last replication, here should check this opened journal's size&state;
        // and replicate should not step to next journal when the pre one is still opened
            if(pre_info->replicating){
                return nullptr;
            }
            std::shared_ptr<RepTask> task(new RepTask);
            task->vol_id = vol_id_;
            task->status = T_WAITING;
            task->info = pre_info;
            // update journal info status
            JournalMeta meta;
            RESULT res = JournalMetaHandle::instance().get_journal_meta(pre_info->key,meta);
            task->info->is_opened = meta.status() == huawei::proto::OPENED? true:false;
            task->info->pos = pre_info->end;
            task->info->end = MAX_JOURNAL_SIZE; // client fill the end offset when task finished
            task->info->replicating = true;
            task->seq_id = seq_id_++;
            task->tp = system_clock::now();
            tp_ = task->tp;
            tasks_.push_back(task);
            return task;
        }
        if(pending_journals_.size() < max_pending_tasks_/2){// pre-fetch journals to promote priority
            std::list<std::string> list;
            RESULT res = meta_->get_consumable_journals(uuid_,vol_id_,
                max_consuming_marker_,max_pending_tasks_,list);
            if(DRS_OK != res){
                LOG_ERROR << "get consumbale_journals failed" << vol_id_;
                return nullptr;
            }
            for(std::string& s:list){
                pending_journals_.push_back(std::move(s));
            }
            if(!pending_journals_.empty())
                max_consuming_marker_.set_cur_journal(pending_journals_.back());
        }
        if(pending_journals_.empty())
            return nullptr;
        std::shared_ptr<RepTask> task(new RepTask);
        task->vol_id = vol_id_;
        task->status = T_WAITING;
        task->info.reset(new JournalInfo());
        task->info->key = pending_journals_.front();
        task->info->pos = 0;
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance().get_journal_meta(task->info->key,meta);
        DR_ASSERT(res == DRS_OK);
        task->info->path = mount_path_ + meta.path();
        task->info->is_opened = meta.status() == huawei::proto::OPENED? true:false;
        task->info->end = MAX_JOURNAL_SIZE;
        task->info->replicating = true;
        task->seq_id = seq_id_++;
        task->tp = system_clock::now();
        tp_ = task->tp;
        pre_info = task->info;
        pending_journals_.pop_front();
        tasks_.push_back(task);  
        return task;        
    }
    
    void recycle_task(){
        // TODO:sort tasks with journal counter & end offset
        std::shared_ptr<RepTask> task(nullptr);
        std::lock_guard<std::mutex> lck(mtx_);
        int i=0;
        for(;i<tasks_.size();i++){
            if(tasks_[i]->status != T_DONE){
                break;
            }
            task = tasks_[i];
            task->info->replicating = false;
        }
        if(!task){
//            LOG_DEBUG << "some previous task may not done yet";
            return;
        }
        if(task->info->key.compare(consuming_marker_.cur_journal()) > 0
            || (task->info->key.compare(consuming_marker_.cur_journal()) == 0 
                && task->info->end > consuming_marker_.pos())){
            consuming_marker_.set_cur_journal(task->info->key);
            consuming_marker_.set_pos(task->info->end);
            RESULT res = meta_->update_journal_marker(uuid_,task->vol_id,
                REPLICATER,consuming_marker_);
            if(res != DRS_OK){
                LOG_WARN << "update marker failed" << consuming_marker_.cur_journal();
            }
            
            LOG_INFO << "update " << task->vol_id << " replicator consuming marker at "
                << task->info->key << ":" << task->info->end;
        }
        else if(task->info->key.compare(consuming_marker_.cur_journal()) == 0 
                && task->info->end == consuming_marker_.pos()){
            LOG_DEBUG << "new consuming marker the same as the last one:"
                << task->info->key << ":" << task->info->end << "<"
                << consuming_marker_.cur_journal() << ":" << consuming_marker_.pos();
        }
        else
            DR_ERROR_OCCURED();
        // remove finished tasks
        tasks_.erase(tasks_.begin(),tasks_.begin()+i);
    }
    uint64_t calculate_composite_priority(){
        return priority_;
    }
    void set_enable(bool enable){
        std::lock_guard<std::mutex> lck(mtx_);
        enable_ = enable;
    }
    void set_delete(bool del){
        std::lock_guard<std::mutex> lck(mtx_);
        deleting_ = del;
    }
    bool get_enable(){
        return enable_;
    }
    bool get_delete(){
        return deleting_;
    }
    int get_priority(){
        return priority_;
    }
    void set_priority(int p){
        priority_ = p;
    }
    int get_pending_journals_size(){
        return pending_journals_.size();
    }
    system_clock::time_point& get_last_task_tp(){
        return tp_;
    }
private:
    bool init_markers(){
        RESULT res;
        res = meta_->get_journal_marker(uuid_,vol_id_,REPLICATER,&consuming_marker_);
        DR_ASSERT(res == DRS_OK);
        max_consuming_marker_.CopyFrom(consuming_marker_);
        pending_journals_.clear();
        // TODO: producer marker, to avoid excessive reading if end marker is set??
        JournalMeta meta;
        res = JournalMetaHandle::instance().get_journal_meta(consuming_marker_.cur_journal(),meta);
        if(res != DRS_OK){
            LOG_WARN << "get nournal meta failed:" << consuming_marker_.cur_journal();
            return false;
        }
        pre_info.reset(new JournalInfo());
        pre_info->key = consuming_marker_.cur_journal();
        pre_info->pos = consuming_marker_.pos();
        pre_info->path = mount_path_ + meta.path();
        pre_info->is_opened = true; // init it with true, then add the journal to next task
        pre_info->end = pre_info->pos;
        pre_info->replicating = false;
        return true;
    }
    std::string vol_id_;
    std::string uuid_;
    JournalMarker consuming_marker_;
    JournalMarker producing_marker_;
    JournalMarker max_consuming_marker_;
    std::shared_ptr<JournalInfo> pre_info;
    uint64_t seq_id_;
    std::vector<std::shared_ptr<RepTask>> tasks_;
    int max_pending_tasks_; // use for consuming slide window
    std::list<std::string> pending_journals_;
    bool enable_;
    bool deleting_;
    int priority_;
    std::mutex mtx_;
    std::shared_ptr<CephS3Meta> meta_;
    std::string mount_path_;
    system_clock::time_point tp_;
};
#endif
