/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    replicate.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REPLICATE_H_
#define REPLICATE_H_
#include <map>
#include <thread>
#include <mutex>
#include <chrono>
#include "../ceph_s3_meta.h"
#include "rpc/consumer.pb.h"
#include "rep_client.h"
#include "common/blocking_queue.h"
#include "rep_volume.h"
#include "common/thread_pool.h"

class Replicate{
private:
    bool enable_;
    bool running_;
    std::mutex volume_mtx_;
    std::string uuid_;
    std::string mount_path_;
    std::shared_ptr<CephS3Meta> meta_;
    std::map<std::string,std::shared_ptr<RepVolume>> volumes_; // keep volume marker and tasks
    BlockingQueue<std::shared_ptr<RepTask>> queue_; // push the task when finished, and active the recycle_thread
    BlockingQueue<JournalMarker> markers_queue_;
    std::list<std::shared_ptr<RepVolume>> priority_list_; // sort volume by priority
    std::unique_ptr<RepClient> client_; // send data over network
    std::unique_ptr<sg_threads::ThreadPool> dispatch_pool_; // generate tasks of replication
    std::unique_ptr<std::thread> work_thread_;// distribute replicate tasks
    std::unique_ptr<std::thread> recycle_thread_; // handle finished tasks
    std::unique_ptr<std::thread> marker_thread_; // sync markers to destination
public:
    Replicate(std::shared_ptr<CephS3Meta> meta,
            const string& mount, const string& addr,
            const std::shared_ptr<grpc::ChannelCredentials>& creds);
    ~Replicate();
    int add_volume(const std::string& vol_id);
    int remove_volume(const std::string& vol_id);
    void task_end_callback(std::shared_ptr<RepTask>);
private:
    void recycle_tasks();
    void sync_markers();
    void run();
    void wait_for_client_ready();
    void dispatch(std::shared_ptr<RepVolume> rep_vol);// dispatch volume to thread pool
};

#endif
