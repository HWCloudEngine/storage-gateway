/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    snapshot_rpccli.cc
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot rpc client to server
* *************************************************/
#include <stdlib.h>
#include <unistd.h>
#include "common/utils.h"
#include "common/config_option.h"
#include "snapshot_rpccli.h"

using huawei::proto::inner::CreateReq;
using huawei::proto::inner::CreateAck;
using huawei::proto::inner::ListReq;
using huawei::proto::inner::ListAck;
using huawei::proto::inner::QueryReq;
using huawei::proto::inner::QueryAck;
using huawei::proto::inner::DeleteReq;
using huawei::proto::inner::DeleteAck;
using huawei::proto::inner::CowReq;
using huawei::proto::inner::CowAck;
using huawei::proto::inner::UpdateReq;
using huawei::proto::inner::UpdateAck;
using huawei::proto::inner::CowUpdateReq;
using huawei::proto::inner::CowUpdateAck;
using huawei::proto::inner::RollbackReq;
using huawei::proto::inner::RollbackAck;
using huawei::proto::inner::DiffReq;
using huawei::proto::inner::DiffAck;
using huawei::proto::inner::ReadReq;
using huawei::proto::inner::ReadAck;
using huawei::proto::inner::SyncReq;
using huawei::proto::inner::SyncAck;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;
using huawei::proto::inner::UpdateEvent;

SnapRpcCli::SnapRpcCli() {
}

SnapRpcCli::~SnapRpcCli() {
}

#define rpc_call(op, req, ack) do {             \
    uint32_t try_times = 0;                     \
    constexpr uint32_t max_try_times = 5;       \
    constexpr uint32_t max_try_interval = 5;    \
    ClientContext ctx;                          \
    while (try_times < max_try_times) {         \
        status_ = stub_->op(&ctx, req, &ack);   \
        if (status_.ok()) {                     \
            break;                              \
        }                                       \
        auto state = channel_->GetState(false); \
        if (state != GRPC_CHANNEL_SHUTDOWN) {   \
            break;                              \
        }                                       \
        LOG_INFO << "retry rpc connection:" << try_times;  \
        sleep(max_try_interval);                \
        try_times++;                            \
    }                                           \
} while(0)

StatusCode SnapRpcCli::do_init_sync(const std::string& vol_name, std::string& latest_snap) {
    LOG_INFO << "do init synchronize snapshot state";
    SyncReq req;
    SyncAck ack;
    req.set_vol_name(vol_name);
    rpc_call(Sync, req, ack);
    latest_snap = ack.latest_snap_name();
    LOG_INFO << "do init synchronize snapshot state, active snaphsot:" << latest_snap
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_list(const std::string& vol_name,
                               std::set<std::string>& snap_set) {
    LOG_INFO << "do list snapshot vname:" << vol_name;
    ListReq req;
    ListAck ack;
    req.set_vol_name(vol_name);
    rpc_call(List, req, ack);
    int snap_num = ack.snap_name_size();
    for (int i = 0; i < snap_num; i++) {
        snap_set.insert(ack.snap_name(i));
    }
    LOG_INFO << "do list snapshot vname:" << vol_name
             << " snap_size:" << ack.snap_name_size() << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_query(const std::string& vol_name,
                                const std::string snap_name,
                                SnapStatus& snap_status) {
    LOG_INFO << "do query snapshot vname:" << vol_name << " sname:" << snap_name;
    QueryReq req;
    QueryAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    rpc_call(Query, req, ack);
    snap_status = ack.snap_status();
    LOG_INFO << "do query snapshot vname:" << vol_name << " sname:" << snap_name 
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_rollback(const SnapReqHead& shead,
                                   const std::string& vol_name, 
                                   const std::string& snap_name, 
                                   std::vector<RollBlock>& blocks) {
    LOG_INFO << "do rollback snapshot vname:" << vol_name << " sname:" << snap_name;
    RollbackReq req;
    RollbackAck ack;
    req.mutable_header()->CopyFrom(shead);
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    rpc_call(Rollback, req, ack);
    int roll_blk_num = ack.roll_blocks_size();
    for (int i = 0; i < roll_blk_num; i++) {
        RollBlock roll_block = ack.roll_blocks(i);
        blocks.push_back(roll_block); 
    }
    LOG_INFO << "do rollback snapshot vname:" << vol_name << " sname:" << snap_name
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_create(const SnapReqHead& shead,
                                 const std::string& vol_name,
                                 const std::string& snap_name) {
    LOG_INFO << "do create snap vol:" << vol_name << " snap_name:" << snap_name;
    CreateReq req;
    CreateAck ack;
    req.mutable_header()->CopyFrom(shead);
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    rpc_call(Create, req, ack);
    LOG_INFO << "do create snap vol:" << vol_name << " snap_name:" << snap_name
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_delete(const SnapReqHead& shead,
                                 const std::string& vol_name,
                                 const std::string& snap_name) {
    LOG_INFO << "do delte snap vol:" << vol_name << " snap_name:" << snap_name;
    DeleteReq req;
    DeleteAck ack;
    req.mutable_header()->CopyFrom(shead);
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    rpc_call(Delete, req, ack);
    LOG_INFO << "do delte snap vol:" << vol_name << " snap_name:" << snap_name
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_update(const SnapReqHead& shead,
                                 const std::string& vol_name,
                                 const std::string& snap_name,
                                 const UpdateEvent& snap_event,
                                 std::string& latest_snap) {
    LOG_INFO << "do update snap_name:" << snap_name << " event:" << snap_event;
    UpdateReq req;
    UpdateAck ack;
    req.mutable_header()->CopyFrom(shead);
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    req.set_snap_event(snap_event);
    rpc_call(Update, req, ack);
    latest_snap = ack.latest_snap_name();
    LOG_INFO << "do update snap_name:" << snap_name << " event:" << snap_event
             << " latest_snap:" << latest_snap << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_cow_check(const std::string& vol_name,
                                    const std::string& snap_name,
                                    const uint64_t block_no,
                                    cow_op_t& cow_op,
                                    std::string& blk_url) {
    LOG_INFO << "do cow check snap_name:" << snap_name << " blk:" << block_no;
    CowReq req;
    CowAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    req.set_blk_no(block_no);
    rpc_call(CowOp, req, ack);
    cow_op = (cow_op_t)ack.cow_op();
    blk_url = ack.blk_url();
    LOG_INFO << "do cow check snap_name:" << snap_name << " blk:" << block_no
             << " cow_op:" << cow_op << " blk_url:" << blk_url << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_cow_update(const std::string& vol_name,
                                     const std::string& snap_name,
                                     const uint64_t    block_no,
                                     const bool        block_zero,
                                     const std::string block_url) {
 
    LOG_INFO << "do cow update snap_name:" << snap_name << " block_no:" << block_no
             << " block_zero:" << block_zero << " block_url:" << block_url;
    CowUpdateReq req;
    CowUpdateAck ack;
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    req.set_blk_no(block_no);
    req.set_blk_zero(block_zero);
    req.set_blk_url(block_url);
    rpc_call(CowUpdate, req, ack);
    LOG_INFO << "do cow update snap_name:" << snap_name << " block_no:" << block_no
             << " block_zero:" << block_zero << " block_url:" << block_url
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_diff(const SnapReqHead& shead,
                               const std::string& vol_name,
                               const std::string& first_snap,
                               const std::string& last_snap,
                               std::vector<DiffBlocks>& blocks) {
    LOG_INFO << "do diff snapshot vname:" << vol_name
             << " first_snap:" << first_snap << " last_snap:"  << last_snap;
    DiffReq req;
    DiffAck ack;
    req.mutable_header()->CopyFrom(shead);
    req.set_vol_name(vol_name);
    req.set_first_snap_name(first_snap);
    req.set_last_snap_name(last_snap);
    rpc_call(Diff, req, ack);
    int diff_blocks_num = ack.diff_blocks_size();
    for (int i = 0; i < diff_blocks_num; i++) {
        DiffBlocks idiff_blocks = ack.diff_blocks(i);
        blocks.push_back(idiff_blocks);
    }
    LOG_INFO << "do diff snapshot vname:" << vol_name
             << " first_snap:" << first_snap << " last_snap:"  << last_snap
             << " ret:" << ack.header().status();
    return ack.header().status();
}

StatusCode SnapRpcCli::do_read(const SnapReqHead& shead, const std::string& vol_name,
                               const std::string& snap_name, const off_t off,
                               const size_t len,
                               std::vector<ReadBlock>& blocks) {

    LOG_INFO << "do read snapshot vname:" << vol_name << " sname:" << snap_name
             << " off:" << off << " len:" << len;
    ReadReq req;
    ReadAck ack;
    req.mutable_header()->CopyFrom(shead);
    req.set_vol_name(vol_name);
    req.set_snap_name(snap_name);
    req.set_off(off);
    req.set_len(len);
    rpc_call(Read, req, ack);
    int read_blk_num = ack.read_blocks_size();
    for (int i = 0; i < read_blk_num; i++) {
        blocks.push_back(ack.read_blocks(i));
    }
    LOG_INFO << "do read snapshot vname:" << vol_name << " sname:" << snap_name
             << " off:" << off << " len:" << len << " read_blk_num:" << read_blk_num
             << " ret:" << ack.header().status();
    return ack.header().status();
}

void SnapRpcCli::init(std::shared_ptr<Channel> channel){
    channel_ = channel;
    stub_.reset(new SnapshotInnerControl::Stub(channel_));
}

