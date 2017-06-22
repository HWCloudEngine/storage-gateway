/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicator_context.h
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
#include "rep_type.h"
#include "rep_task.h"
#include "sg_server/consumer_interface.h"
#include "sg_server/journal_meta_manager.h"
#include "common/config_option.h"
using huawei::proto::JournalElement;
class ReplicatorContext:public IConsumer {
    // TODO: replace task windows with general sequential queue
    class TaskWindow{
        uint64_t last_acked_id;
        uint64_t last_sent_id;
        std::shared_ptr<TransferTask> last_acked_task;
        std::shared_ptr<TransferTask> last_sent_task;
        std::vector<std::shared_ptr<TransferTask>> tasks;
        int window_size;
    public:
        explicit TaskWindow(int size):
            window_size(size),
            last_acked_id(0L),
            last_sent_id(0L),
            last_sent_task(nullptr),
            last_acked_task(nullptr){}
        ~TaskWindow(){}
        bool has_free_slot(){
           return last_sent_id - last_acked_id < window_size;
        }
        std::shared_ptr<TransferTask> get_failed_task(){
            if(tasks.empty())
                return nullptr;
            for(int i=0;i<tasks.size();i++){
                if(tasks[i]->get_status() == T_ERROR){
                    return tasks[i];
                }
            }
            return nullptr;
        }
        std::shared_ptr<TransferTask>& get_last_sent_task(){
            return last_sent_task;
        }
        std::shared_ptr<TransferTask>& get_last_acked_task(){
            return last_acked_task;
        }
        bool add_task(std::shared_ptr<TransferTask>& task){
            if(task->get_id() <= last_acked_id 
                || task->get_id() - last_acked_id > window_size){
                return false; // out of task window
            }
            last_sent_task = task;
            last_sent_id = task->get_id();
            tasks.push_back(last_sent_task);
            return true;
        }
        bool ack_task(std::shared_ptr<TransferTask>& task){
            if(task->get_id() <= last_acked_id || task->get_id() > last_sent_id)// t id is not in window range
                return false;
            if(task->get_status() != T_DONE)
                return false;
            bool is_window_moved = false;
            while(!tasks.empty()){
                if(tasks.front()->get_status() == T_DONE){
                    last_acked_task = tasks.front();
                    last_acked_id = last_acked_task->get_id();
                    tasks.erase(tasks.begin());
                    is_window_moved = true;
                }
                else
                    break;
            }
            return is_window_moved;
        }

        int clear(){
            for(auto it=tasks.begin(); it!=tasks.end(); ++it){
                if((*it)->get_status() != T_DONE){
                    return -1;
                }
            }
            tasks.clear();
            last_acked_id = last_sent_id;
            last_sent_task = (nullptr);
            last_acked_task = (nullptr);
            return 0;
        }

        void cancel_all_tasks(){
            for(auto it=tasks.begin(); it!=tasks.end(); ++it){
                if((*it)->get_status() != T_DONE){
                    (*it)->cancel();
                }
            }
            tasks.clear();
            last_acked_id = last_sent_id;
            last_sent_task = (nullptr);
            last_acked_task = (nullptr);
        }

    };

private:
    void recycle_task(std::shared_ptr<TransferTask>& t);
    std::shared_ptr<TransferTask> construct_task(const JournalElement& e);

public:
    ReplicatorContext(const std::string& vol,
            std::shared_ptr<JournalMetaManager> j_meta_mgr):
            IConsumer(vol),
            j_meta_mgr_(j_meta_mgr),
            task_window_(MAX_TASK_PER_VOL),
            seq_id_(0L),
            initialized_(false){
        init();
    }
    ~ReplicatorContext(){}
    // init resources:markers, slide windows
    int init();
    // cancel tasks
    int cancel_all_tasks();
    // generate next task
    std::shared_ptr<TransferTask> get_next_replicate_task();
    // whether has journals which need to replicate
    bool has_journals_to_transfer();

    const string& get_peer_volume();
    void set_peer_volume(const string& peer_vol);

    // get producer marker,replicator should not tranfer journals more than this marker
    int get_producer_marker(JournalMarker& marker);
    virtual CONSUMER_TYPE get_type();
    virtual int get_consumer_marker(JournalMarker& marker);
    virtual int update_consumer_marker(const JournalMarker& marker);
    virtual int get_consumable_journals(const JournalMarker& marker,
            const int limit, std::list<JournalElement>& list);
private:
    // journal meta manager
    std::shared_ptr<JournalMetaManager> j_meta_mgr_;
    // set max task number in consuming slide window
    int max_pending_tasks_;
    // replicator temp consumer marker,which is used to mark max journal
    // in transmission
    JournalMarker temp_c_marker_;
    // replicator cached comsumer marker
    JournalMarker c_marker_;
    bool initialized_;
    // sequence id for rep tasks
    uint64_t seq_id_;
    std::mutex mtx_;
    TaskWindow task_window_;
    // prefetched consumable journals
    std::list<JournalElement> pending_journals_;
    // peer volume
    string peer_volume_;
};

typedef struct MarkerContext{
    ReplicatorContext* rep_ctx;
    JournalMarker marker;
    MarkerContext(ReplicatorContext* _rep_ctx,
            const JournalMarker& _marker):
            rep_ctx(_rep_ctx),
            marker(_marker){}
}MarkerContext;

#endif
