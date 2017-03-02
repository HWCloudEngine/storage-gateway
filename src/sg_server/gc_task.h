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
#include <map>
#include <set>
#include <atomic>
#include "common/ceph_s3_lease.h"
#include "common/config.h"
#include "journal_gc_manager.h"
#include "journal_meta_manager.h"
#include "consumer_interface.h"

class GCTask{
typedef std::map<std::string,std::set<IConsumer*>> vc_map_t;
typedef std::pair<std::string,std::set<IConsumer*>> vc_pair_t;
private:
    std::shared_ptr<JournalGCManager> gc_meta_ptr_;
    std::shared_ptr<JournalMetaManager> j_meta_ptr_;
    std::unique_ptr<CephS3LeaseServer> lease_;
    std::string mount_path_;
    std::unique_ptr<std::thread> thread_GC_;
    int tick_;
    // scaning all volume journals for garbage collection at this regular interval
    int GC_window_;
    // check volumes leases at this regular interval
    int lease_check_window_;
    bool GC_running_;
    // whether initialization is done(all consumers registered?)?
    bool volumes_initialized;
     // volumes that need do GC & recycle neglected opened journals
    vc_map_t vols_;
    std::mutex mtx_;
public:
    int init(const Configure& conf, std::shared_ptr<JournalGCManager> gc_meta,
            std::shared_ptr<JournalMetaManager> j_meta);
    static GCTask& instance(){
        static GCTask task;
        return task;
    };
    GCTask(GCTask&) = delete;
    GCTask& operator=(GCTask const&) = delete;
    void register_consumer(const std::string& vol_id,IConsumer* c);
    void unregister_consumer(const std::string& vol_id,const CONSUMER_TYPE& type);

    int add_volume(const std::string &vol_id);
    int remove_volume(const std::string &vol_id);

    void set_volumes_initialized(bool f);
private:
    GCTask():
        volumes_initialized(false),
        GC_running_(false){
    };
    ~GCTask(){
        GC_running_ = false;
        clean_up();
        if(thread_GC_.get() && thread_GC_->joinable())
            thread_GC_->join();
    };
    void do_GC();
    void lease_check_task();
    void clean_up();
    int get_least_marker(const string& vol,std::set<IConsumer*>& set,
            JournalMarker& marker);
};
#endif
