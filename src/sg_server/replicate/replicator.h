/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicator.h
* Author: 
* Date:         2017/01/17
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REPLICATOR_H_
#define REPLICATOR_H_
#include <string>
#include <mutex>
#include "rpc/common.pb.h"
#include "rep_transmitter.h"
#include "rep_type.h"
#include "../consumer_interface.h"
#include "../journal_meta_manager.h"
using huawei::proto::JournalElement;
class ReplicatorContext:public IConsumer {
    // TODO: replace task windows with general sequential queue
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
            last_sent_id(0L),
            last_sent_task(nullptr),
            last_acked_task(nullptr){}
        ~TaskWindow(){}
        bool has_free_slot(){
           return last_sent_id - last_acked_id < window_size;
        }
        std::shared_ptr<RepTask> get_failed_task(){
            if(tasks.empty())
                return nullptr;
            for(int i=0;i<tasks.size();i++){
                if(tasks[i]->status == T_ERROR){
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

private:
    bool init_markers();
    std::shared_ptr<RepTask> get_next_replicate_task();
    void recycle_task(std::shared_ptr<RepTask>& t);
    std::shared_ptr<RepTask> construct_task(const JournalElement& e);

public:
    ReplicatorContext(const std::string& vol,
            std::shared_ptr<JournalMetaManager> j_meta_mgr,
            const std::string& mount):
            IConsumer(vol),
            j_meta_mgr_(j_meta_mgr),
            mount_path_(mount),
            task_window_(MAX_TASK_PER_VOL),
            seq_id_(0L){
        init_markers();
    }
    ~ReplicatorContext(){}
    // launch to submit specified count of tasks
    void submit_tasks();
    // whether has journals which need to replicate
    bool has_journals_to_transfer();
    // marker that has been tranferred, but not synced to meta data server(not persisted)
    const JournalMarker& get_transferring_marker();
    // get producer marker,replicator should not tranfer journals more than this marker
    int get_producer_marker(JournalMarker& marker);
    virtual CONSUMER_TYPE get_type();
    virtual int get_consumer_marker(JournalMarker& marker);
    virtual int update_consumer_marker(const JournalMarker& marker);
    virtual int get_consumable_journals(const JournalMarker& marker,
            const int limit, std::list<JournalElement>& list);
private:
    std::string mount_path_;
    // journal meta manager
    std::shared_ptr<JournalMetaManager> j_meta_mgr_;
    // set max task number in consuming slide window
    int max_pending_tasks_;
    // replicator cached comsumer marker
    JournalMarker c_marker_;
    // replicator temp consumer marker,which is used to mark max journal
    // in transmission
    JournalMarker temp_c_marker_;
    // sequence id for rep tasks
    uint64_t seq_id_;
    std::mutex mtx_;
    TaskWindow task_window_;
    // prefetched consumable journals
    std::list<JournalElement> pending_journals_;
};
#endif
