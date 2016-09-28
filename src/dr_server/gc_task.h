/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    gc_task.h
* Author: 
* Date:         2016/09/07
* Version:      1.0
* Description:  recycle comsumed journals and seal the journals of crashed writers
* 
************************************************/
#ifndef GC_TASK_H_
#define GC_TASK_H_
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <set>
#include "ceph_s3_lease.h"
#include "journal_gc_manager.h"
class GCTask{
private:
    std::shared_ptr<JournalGCManager> meta_ptr_;
    std::unique_ptr<CephS3LeaseServer> lease_;
    std::string mount_path_;
    std::unique_ptr<std::thread> thread_GC_;
    int tick_;
    int GC_window_;
    int lease_check_window_;
    bool GC_running_;
    std::set<string> vols_;
    std::set<string> vol_to_add_;
    std::set<string> vol_to_remove_;
    std::mutex add_mutex_;
    std::mutex remove_mutex_;
    std::mutex mtx_;
public:
    int init(std::shared_ptr<JournalGCManager> meta);
    int add_volume(const std::string &vol_id);
    int remove_volume(const std::string &vol_id);
    static GCTask& instance(){
        static GCTask task;
        return task;
    };
    GCTask(GCTask&) = delete;
    GCTask& operator=(GCTask const&) = delete;
private:
    GCTask(){};
    ~GCTask(){
        GC_running_ = false;
        if(thread_GC_.get() && thread_GC_->joinable())
            thread_GC_->join();
    };
    void GC_task();
    void lease_check_task();
    void update_volume_set();
};
#endif
