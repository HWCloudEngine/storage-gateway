/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_type.h
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_TYPE_H_
#define REP_TYPE_H_
#include <cstdint>
#include <string>
#include <memory>
#include <chrono>
#include <functional>
#include "rpc/journal.pb.h"
#include "rpc/common.pb.h"
#define DESPATCH_THREAD_CNT 2
#define MAX_TASK_PER_VOL 10
#define MAX_JOURNAL_SIZE (1024*1024*64) // TODO:read from config
#define MAX_REPLICATE_TASK  16 // TODO: read from config

using huawei::proto::JournalMeta;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::NO_SUCH_KEY;
using huawei::proto::OPENED;
using huawei::proto::SEALED;
using google::protobuf::int64;
typedef enum TASK_STATUS{
    T_UNKNOWN,
    T_WAITING,
    T_RUNNING,
    T_DONE,
    T_ERROR
}TASK_STATUS;

typedef struct JournalInfo{
    JournalInfo():
        key(""),path(""),is_opened(true),
        pos(0),end(0){}
    ~JournalInfo(){}
    JournalInfo& operator=(JournalInfo const& info){
        key = info.key;
        path = info.path;
        is_opened = info.is_opened;
        pos = info.pos;
        end = info.end;
    }

    std::string key;
    std::string path;
    bool is_opened;
    int pos;
    int end;
}JournalInfo;

typedef struct RepTask{
    bool operator<(RepTask const& task2){
        return id < task2.id;
    }
    uint64_t id;
    std::string vol_id;
    JournalInfo info;
    TASK_STATUS status;
    std::chrono::system_clock::time_point tp;
    std::function<void(std::shared_ptr<RepTask>)> callback;
}RepTask;
#endif