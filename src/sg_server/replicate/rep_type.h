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
#include <functional>
#include "rpc/journal.pb.h"
#include "rpc/common.pb.h"
#include "rpc/volume.pb.h"
#define DESPATCH_THREAD_CNT 2
#define MAX_TASK_PER_VOL 10

using huawei::proto::JournalMeta;
using huawei::proto::RESULT;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::NO_SUCH_KEY;
using huawei::proto::OPENED;
using huawei::proto::SEALED;
using google::protobuf::int64;
//replication operation
using huawei::proto::ReplicateOperation;
using huawei::proto::REPLICATION_CREATE;
using huawei::proto::REPLICATION_ENABLE;
using huawei::proto::REPLICATION_DISABLE;
using huawei::proto::REPLICATION_FAILOVER;
using huawei::proto::REPLICATION_REVERSE;
using huawei::proto::REPLICATION_QUERY;
using huawei::proto::REPLICATION_DELETE;
using huawei::proto::REPLICATION_TEST;
using huawei::proto::REPLICATION_LIST;
// define replication status
using huawei::proto::RepStatus;
using huawei::proto::REP_UNKNOW;
using huawei::proto::REP_CREATING;
using huawei::proto::REP_ENABLING;
using huawei::proto::REP_ENABLED;
using huawei::proto::REP_DISABLING;
using huawei::proto::REP_DISABLED;
using huawei::proto::REP_FAILING_OVER;
using huawei::proto::REP_FAILED_OVER;
using huawei::proto::REP_REVERSING;
using huawei::proto::REP_DELETING;
using huawei::proto::REP_DELETED;
using huawei::proto::REP_ERROR;

#endif