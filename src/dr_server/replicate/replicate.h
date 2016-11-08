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
#include "rep_volume.hpp"
#include "common/thread_pool.hpp"

class Replicate:public huawei::proto::Replicator::Service{
private:
    bool enable_;
    bool running_;
    std::unique_ptr<sg_threads::ThreadPool> despatch_pool_; 
    std::unique_ptr<std::thread> work_thread_;// distribute replicate tasks
    std::unique_ptr<std::thread> recycle_thread_; // handle finished tasks
    std::shared_ptr<CephS3Meta> meta_;
    std::string mount_path_;
    std::map<std::string,std::shared_ptr<RepVolume>> volumes_; // keep volume marker and tasks
    std::mutex volume_mtx_;
    std::string uuid_;
    std::unique_ptr<RepClient> client_; // send data over network
    BlockingQueue<std::shared_ptr<RepTask>> queue_; // push the task when finished, and active the recycle_thread
public:
    Replicate(std::shared_ptr<CephS3Meta> meta,
            const string& mount, const string& addr,
            const std::shared_ptr<grpc::ChannelCredentials>& creds);
    ~Replicate();
    int add_volume(const std::string& vol_id);
    int remove_volume(const std::string& vol_id);
    void task_end_callback(std::shared_ptr<RepTask>);
//    int enable_replicate();
//    int disable_replicate();
//    int disable_volume_replicate(const std::string& vol_id);
//    int resume_volume_replicate(const std::string& vol_id);
private:
    void recycle_tasks();
    void run();
    void wait_for_client_ready();
    void despatch(std::shared_ptr<RepVolume> rep_vol);
};

#endif
