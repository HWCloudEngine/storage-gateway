/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_scheduler.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_SCHEDULER_H_
#define REP_SCHEDULER_H_
#include <map>
#include <thread>
#include <mutex>
#include "../ceph_s3_meta.h"
#include "rpc/consumer.pb.h"
#include "rep_transmitter.h"
#include "common/blocking_queue.h"
#include "rep_volume.h"
#include "common/thread_pool.h"

class RepScheduler{
private:
    bool enable_;
    bool running_;
    std::mutex volume_mtx_;
    std::string uuid_;
    std::string mount_path_;
    std::shared_ptr<CephS3Meta> meta_;
    // volumes which need synced
    std::map<std::string,std::shared_ptr<RepVolume>> volumes_;
    // sorted volume by priority
    std::list<std::shared_ptr<RepVolume>> priority_list_;
    // work thread pool for replicating tasks
    std::unique_ptr<sg_threads::ThreadPool> dispatch_pool_;
    // work thread to dispatch rep_volume
    std::unique_ptr<std::thread> work_thread_;
public:
    RepScheduler(std::shared_ptr<CephS3Meta> meta,
            const string& mount);
    ~RepScheduler();
    int add_volume(const std::string& vol_id);
    int remove_volume(const std::string& vol_id);
    void notify_rep_state_changed(const string& vol);
private:
    void run();
    void wait_for_client_ready();
    // dispatch volume to thread pool
    void dispatch(std::shared_ptr<RepVolume> rep_vol);
};

#endif
