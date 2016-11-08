/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_client.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_CLIENT_H_
#define REP_CLIENT_H_
#include <list>
#include <vector>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "rpc/replicator.grpc.pb.h"
#include "rpc/replicator.pb.h"
#include "rpc/common.pb.h"
#include "rpc/journal.pb.h"
#include "rep_type.h"
using huawei::proto::JournalMarker;
using grpc::Channel;
using huawei::proto::ReplicateRequest;
using huawei::proto::ReplicateResponse;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
typedef grpc_connectivity_state ClientState;
#define CLIENT_IDLE         GRPC_CHANNEL_IDLE
#define CLIENT_CONNECTING   GRPC_CHANNEL_CONNECTING
#define CLIENT_READY        GRPC_CHANNEL_READY
#define CLIENT_FAILURE      GRPC_CHANNEL_TRANSIENT_FAILURE
#define CLIENT_SHUTDOWN     GRPC_CHANNEL_SHUTDOWN

typedef struct RepTask{
    int id;
    int priority;
    std::string vol_id;
    std::unique_ptr<std::vector<JournalInfo>> journals;
    int result;
    TASK_STATUS status;
    std::unique_ptr<ClientReaderWriter<ReplicateRequest,
        ReplicateResponse>> stream;
    std::unique_ptr<std::thread> send;
}RepTask;

class RepClient{
private:
    std::unique_ptr<huawei::proto::Replicator::Stub> stub_;
    int unique_id_;
    int running_tasks_;
    int max_tasks_;
    std::map<int,std::shared_ptr<RepTask>> task_map_;
    std::mutex mtx_;
    std::shared_ptr<Channel> channel_;
public:
    RepClient(std::shared_ptr<Channel> channel,int max_tasks); 
    int submit_task(std::shared_ptr<RepTask> task,
            const std::function<void(std::shared_ptr<RepTask>)>& callback);
    TASK_STATUS query_task_status(int task_id);
    ClientState get_state(bool try_to_connect);
    bool wait_for_state_change(const ClientState& state,
            std::chrono::system_clock::time_point deadline);
    std::string get_printful_state(ClientState state);
private:
    void do_replicate(std::shared_ptr<RepTask> task,
            const std::function<void(std::shared_ptr<RepTask>)>& callback);
};

#endif

