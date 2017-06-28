/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    snapshot_rpccli.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot rpc client to server
*  
*************************************************/
#ifndef SRC_SG_CLIENT_SNAPSHOT_SNAPSHOT_RPCCLI_H_
#define SRC_SG_CLIENT_SNAPSHOT_SNAPSHOT_RPCCLI_H_
#include <stdlib.h>
#include <unistd.h>
#include <string>
#include <grpc++/grpc++.h>
#include "log/log.h"
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/snapshot_inner_control.pb.h"
#include "rpc/snapshot_inner_control.grpc.pb.h"
#include "common/define.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::StatusCode;
using huawei::proto::SnapType;
using huawei::proto::SnapStatus;
using huawei::proto::SnapReqHead;
using huawei::proto::DiffBlocks;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;
using huawei::proto::inner::SnapshotInnerControl;
using huawei::proto::inner::UpdateEvent;

class SnapRpcCli {
 public:
    SnapRpcCli();
    SnapRpcCli(const SnapRpcCli& other) = delete;
    SnapRpcCli& operator=(const SnapRpcCli& other) = delete;
    ~SnapRpcCli();

    StatusCode do_init_sync(const std::string& vol_name, std::string& latest_snap);
    StatusCode do_create(const SnapReqHead& shead,
                         const std::string& vol_name,
                         const std::string& snap_name);
    StatusCode do_delete(const SnapReqHead& shead,
                         const std::string& vol_name,
                         const std::string& snap_name);
    StatusCode do_rollback(const SnapReqHead& shead,
                           const std::string& vol_name, 
                           const std::string& snap_name,
                           std::vector<RollBlock>& blocks);
    StatusCode do_update(const SnapReqHead& shead,
                         const std::string& vol_name,
                         const std::string& snap_ame,
                         const UpdateEvent& snap_event,
                         std::string& latest_snap);
    StatusCode do_list(const std::string& vol_name,
                       std::set<std::string>& snap_set);
    StatusCode do_query(const std::string& vol_name,
                        const std::string snap_name,
                        SnapStatus& snap_status);
    StatusCode do_diff(const SnapReqHead& shead,
                       const std::string& vol_name,
                       const std::string& first_snap,
                       const std::string& last_snap,
                       std::vector<DiffBlocks>& blocks);
    StatusCode do_read(const SnapReqHead& shead,
                       const std::string& vol_name,
                       const std::string& snap_name, const off_t off,
                       const size_t len,
                       std::vector<ReadBlock> blocks);
    StatusCode do_cow_check(const std::string& vol_name,
                            const std::string& snap_name,
                            const uint64_t block_no,
                            cow_op_t& cow_op,
                            std::string& blk_url);
    StatusCode do_cow_update(const std::string& vol_name,
                             const std::string& snap_name,
                             const uint64_t    block_no,
                             const bool        block_zero,
                             const std::string block_url);
 
 private:
    void build_channel();
    void build_stub();

 private:
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<SnapshotInnerControl::Stub> stub_;
    ClientContext ctx_;
    Status status_;
};

#endif  // SRC_SG_CLIENT_SNAPSHOT_SNAPSHOT_RPCCLI_H_
