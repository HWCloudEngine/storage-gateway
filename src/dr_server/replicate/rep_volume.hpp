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
    class TaskWindow{
        uint64_t last_acked_id;
        uint64_t last_sent_id;
        std::shared_ptr<RepTask> last_acked_task;
        std::shared_ptr<RepTask> last_sent_task;
        std::vector<std::shared_ptr<RepTask>> tasks;
        int window_size;
    public:
        TaskWindow(int size):
            window_size(size),
            last_acked_id(0L),
            last_sent_id(0L){}
        ~TaskWindow(){}
        bool has_free_slot(){
           return last_sent_id - last_acked_id < window_size;
        }
        std::shared_ptr<RepTask> get_failed_task(){
            if(tasks.empty())
                return nullptr;
            for(int i=0;i<tasks.size();i++){
                if(tasks[i]->status == T_ERROR){
                    tasks[i]->status = T_UNKNOWN;
                    return tasks[i];
                }
            }
            return nullptr;
        }
        std::shared_ptr<RepTask>& get_last_sent_task(){
            return last_sent_task;
        }
        std::shared_ptr<RepTask>& get_last_acked_task(){
            return last_acked_task;
        }
        bool add_task(std::shared_ptr<RepTask>& task){
            if(task->id <= last_acked_id 
                || task->id - last_acked_id > window_size){
                return false; // out of task window
            }
            last_sent_task = task;
            last_sent_id = task->id;
            tasks.push_back(last_sent_task);
            return true;
        }
        bool ack_task(std::shared_ptr<RepTask>& task){
            if(task->id <= last_acked_id || task->id > last_sent_id)// t id is not in window range
                return false;
            if(task->status != T_DONE)
                return false;
            bool is_window_moved = false;
            while(!tasks.empty()){
                if(tasks.front()->status == T_DONE){
                    last_acked_task = tasks.front();
                    last_acked_id = last_acked_task->id;
                    tasks.erase(tasks.begin());
                    is_window_moved = true;
                }
                else
                    break;
            }
            return is_window_moved;
        }
        
    };
public:
    RepVolume(std::string uuid,std::string vol_id,std::string mount,
            std::shared_ptr<CephS3Meta> meta):
            uuid_(uuid),
            vol_id_(vol_id),
            mount_path_(mount),
            meta_(meta),
            max_pending_tasks_(MAX_TASK_PER_VOL),
            enable_(true),
            seq_id_(0L),
            task_window_(max_pending_tasks_){
        init_markers();
    }
    ~RepVolume(){}
    std::shared_ptr<RepTask> get_next_replicate_task(){
        std::lock_guard<std::mutex> lck(mtx_);
        // first check for failed task and redo it if window was full
        if(!task_window_.has_free_slot())
            return task_window_.get_failed_task();
        // TODO:check timeout/failed task when window is not full used
        if(task_window_.get_failed_task())
            return task_window_.get_failed_task();
        if(!task_window_.get_last_sent_task()){//no task sent, check the consuming marker
            if(!init_markers()){
                return nullptr;
            }
            std::shared_ptr<RepTask> task = construct_task();
            if(task){
                task->id = ++seq_id_;
                time_ = task->tp;
                task_window_.add_task(task);
            }
            return task;
        }
        // if last replicating journal is opened, this journal may be appended writes
        // since last replication, here should check this opened journal's state;
        // and replicate should not step to next journal when the pre one is still opened
        if(task_window_.get_last_sent_task()->info.is_opened){
            std::shared_ptr<RepTask>& t = task_window_.get_last_sent_task();
            if(t->status != T_DONE){
                if(t->status == T_ERROR)
                    return task_window_.get_failed_task(); // re-try the task if failed
                return nullptr; // the same opened journal is being replicated
            }
            std::shared_ptr<RepTask> task =
                construct_task(t);
            if(task){
                task->id = ++seq_id_;
                time_ = task->tp;
                task_window_.add_task(task);
            }
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
        std::shared_ptr<RepTask> task = construct_task(pending_journals_.front());
        if(task){
            task->id = ++seq_id_;
            time_ = task->tp;
            task_window_.add_task(task);
            pending_journals_.pop_front();
        }
        return task;        
    }
    // recycle_task, return true if consuming marker was updated
    bool recycle_task(std::shared_ptr<RepTask>& t){
        std::lock_guard<std::mutex> lck(mtx_);
        if(!task_window_.ack_task(t))
            return false;
        std::shared_ptr<RepTask>& task = task_window_.get_last_acked_task();
        if(task->info.key.compare(consuming_marker_.cur_journal()) > 0
            || (task->info.key.compare(consuming_marker_.cur_journal()) == 0 
                && task->info.end > consuming_marker_.pos())){
            consuming_marker_.set_cur_journal(task->info.key);
            consuming_marker_.set_pos(task->info.end);
            RESULT res = meta_->update_journal_marker(uuid_,task->vol_id,
                REPLICATER,consuming_marker_);
            if(res != DRS_OK){
                LOG_WARN << "update marker failed" << consuming_marker_.cur_journal();
            }
            
            LOG_INFO << "update " << task->vol_id << " replicator consuming marker at "
                << task->info.key << ":" << task->info.end;
            return true;
        }
        else if(task->info.key.compare(consuming_marker_.cur_journal()) == 0 
                && task->info.end == consuming_marker_.pos()){
            LOG_DEBUG << "new consuming marker the same as the last one:"
                << task->info.key << ":" << task->info.end << "<"
                << consuming_marker_.cur_journal() << ":" << consuming_marker_.pos();
        }
        else
            DR_ERROR_OCCURED();
        return false;
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
    system_clock::time_point& get_last_task_time(){
        return time_;
    }
    const JournalMarker& get_consuming_marker(){
        return consuming_marker_;
    }
private:
    bool init_markers(){
        RESULT res;
        res = meta_->get_journal_marker(uuid_,vol_id_,REPLICATER,&consuming_marker_);
        if(res != DRS_OK)
            return false;
        max_consuming_marker_.CopyFrom(consuming_marker_);
        pending_journals_.clear();
        // TODO: producer marker, to avoid excessive reading if end marker is set??
        return true;
    }
    std::shared_ptr<RepTask> construct_task(){
        std::shared_ptr<RepTask> task(new RepTask);
        task->vol_id = vol_id_;
        task->status = T_WAITING;
        // update journal info status
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance()
            .get_journal_meta(consuming_marker_.cur_journal(),meta);
        if(DRS_OK != res)
            return nullptr;
        task->info.is_opened = meta.status() == huawei::proto::OPENED? true:false;
        task->info.key = consuming_marker_.cur_journal();
        task->info.path = mount_path_ + meta.path();
        task->info.pos = consuming_marker_.pos();
        task->info.end = MAX_JOURNAL_SIZE; // client fill the end offset when task finished
        task->tp = system_clock::now();
        return task;
    }
    std::shared_ptr<RepTask> construct_task(const std::string& key){
        std::shared_ptr<RepTask> task(new RepTask);
        task->vol_id = vol_id_;
        task->status = T_WAITING;
        task->info.key = key;
        task->info.pos = 0;
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance().get_journal_meta(key,meta);
        DR_ASSERT(res == DRS_OK);
        task->info.path = mount_path_ + meta.path();
        task->info.is_opened = meta.status() == huawei::proto::OPENED? true:false;
        task->info.end = MAX_JOURNAL_SIZE;
        task->tp = system_clock::now();
        return task;
    }
    std::shared_ptr<RepTask> construct_task(std::shared_ptr<RepTask>& t){
        std::shared_ptr<RepTask> task(new RepTask);
        task->vol_id = t->vol_id;
        task->status = T_WAITING;
        task->info.key = t->info.key;
        task->info.pos = t->info.end; // start at journal end of last task
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance().get_journal_meta(t->info.key,meta);
        DR_ASSERT(res == DRS_OK);
        task->info.path = mount_path_ + meta.path();
        task->info.is_opened = meta.status() == huawei::proto::OPENED? true:false;
        task->info.end = MAX_JOURNAL_SIZE;
        task->tp = system_clock::now();
        return task;
    }
//
    std::string vol_id_;
    std::string uuid_;
    JournalMarker consuming_marker_;
    JournalMarker producing_marker_;
    JournalMarker max_consuming_marker_;
    int max_pending_tasks_; // use for consuming slide window
    TaskWindow task_window_;
    std::list<std::string> pending_journals_;
    bool enable_;
    bool deleting_;
    int priority_;
    std::mutex mtx_;
    std::shared_ptr<CephS3Meta> meta_;
    std::string mount_path_;
    system_clock::time_point time_;
    uint64_t seq_id_;
};
#endif
