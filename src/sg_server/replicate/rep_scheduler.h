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
#include "task_handler.h"
#include "common/blocking_queue.h"
#include "rep_volume.h"
#include "common/thread_pool.h"
#include "common/config.h"
class RepScheduler{
private:
    bool enable_;
    bool running_;
    std::mutex volume_mtx_;
    Configure& conf_;
    std::shared_ptr<CephS3Meta> meta_;
    // output queue
    BlockingQueue<std::shared_ptr<RepVolume>>& vol_queue_;
    // volumes which need synced
    std::map<std::string,std::shared_ptr<RepVolume>> volumes_;
    // sorted volume by priority
    std::list<std::shared_ptr<RepVolume>> priority_list_;
    // work thread to sort and dispatch rep_volume
    std::unique_ptr<std::thread> work_thread_;
public:
    RepScheduler(std::shared_ptr<CephS3Meta> meta,
            Configure& conf,
            BlockingQueue<std::shared_ptr<RepVolume>>& vol_queue);
    ~RepScheduler();
    int add_volume(const std::string& vol_id);
    int remove_volume(const std::string& vol_id);
    void notify_rep_state_changed(const string& vol);
private:
    void work();
    void wait_for_client_ready();
};

#endif
