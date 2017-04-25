/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_mgr.cc
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot request dispatch
* 
*************************************************/
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "log/log.h"
#include "snapshot_mgr.h"

using huawei::proto::StatusCode;

#define CMD_PREV(vol, op)            \
do {                                 \
    string log_msg = "SnapshotMgr";  \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    LOG_INFO << log_msg;             \
}while(0)

#define CMD_POST(vol, op, ret)       \
do {                                 \
    string log_msg = "SnapshotMgr";  \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    if (!ret) {                      \
        log_msg += " ok";               \
        LOG_INFO << log_msg;            \
        return grpc::Status::OK;        \
    } else {                            \
        log_msg += " failed";           \
        LOG_INFO << log_msg;            \
        return grpc::Status::CANCELLED; \
    }                                   \
}while(0)

#define CMD_DO(vname, op, req, ack)    \
do {                                   \
    auto it = m_all_snapmds.find(vname);   \
    if (it == m_all_snapmds.end()) {       \
        ret = StatusCode::sVolumeNotExist; \
        break;                         \
    }                                  \
    ret = it->second->op(req, ack);    \
}while(0);

SnapshotMgr::SnapshotMgr() {
    m_all_snapmds.clear();
}

SnapshotMgr::~SnapshotMgr() {
    m_all_snapmds.clear();
}

StatusCode SnapshotMgr::add_volume(const string& vol_name,
                                   const size_t& vol_size) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_all_snapmds.find(vol_name);
    if (it != m_all_snapmds.end()) {
        LOG_INFO << "add volume:" << vol_name << "failed, already exist";
        return StatusCode::sVolumeAlreadyExist;
    }

    shared_ptr<SnapshotMds> snap_mds;
    snap_mds.reset(new SnapshotMds(vol_name, vol_size));
    m_all_snapmds.insert({vol_name, snap_mds});
    snap_mds->recover();

    return StatusCode::sOk;
}

StatusCode SnapshotMgr::del_volume(const string& vol_name) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_all_snapmds.erase(vol_name);
    return StatusCode::sOk;
}

grpc::Status SnapshotMgr::Sync(ServerContext* context, const SyncReq* req,
                               SyncAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Sync");
    CMD_DO(vname, sync, req, ack);
    CMD_POST(vname, "Sync", ret);
}

grpc::Status SnapshotMgr::Create(ServerContext* context, const CreateReq* req,
                                 CreateAck* ack)  {
    StatusCode ret;
    string vname = req->vol_name();
    size_t vsize = req->vol_size();
    auto it = m_all_snapmds.find(vname);
    shared_ptr<SnapshotMds> snap_mds;
    if (it != m_all_snapmds.end()) {
        snap_mds = it->second;
        goto create;
    }
    /*(todo debug only)create snapshotmds for each volume*/
    snap_mds.reset(new SnapshotMds(vname, vsize));
    m_all_snapmds.insert({vname, snap_mds});

create:
    CMD_PREV(vname, "create");
    ret = snap_mds->create_snapshot(req, ack);
    CMD_POST(vname, "create", ret);
}

grpc::Status SnapshotMgr::List(ServerContext* context, const ListReq* req,
                               ListAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "List");
    CMD_DO(vname, list_snapshot, req, ack);
    CMD_POST(vname, "List", ret);
}

grpc::Status SnapshotMgr::Query(ServerContext* context, const QueryReq* req,
                                QueryAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Query");
    CMD_DO(vname, query_snapshot, req, ack);
    CMD_POST(vname, "Query", ret);
}

grpc::Status SnapshotMgr::Delete(ServerContext* context, const DeleteReq* req,
                                 DeleteAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();
    string snap_name = req->snap_name();

    CMD_PREV(vname, "Delete");
    CMD_DO(vname, delete_snapshot, req, ack);
    CMD_POST(vname, "Delete", ret);
}

grpc::Status SnapshotMgr::Rollback(ServerContext* context,
                           const RollbackReq* req, RollbackAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Rollback");
    CMD_DO(vname, rollback_snapshot, req, ack);
    CMD_POST(vname, "Rollback", ret);
}

grpc::Status SnapshotMgr::Update(ServerContext* context, const UpdateReq* req,
                                 UpdateAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Update");
    CMD_DO(vname, update, req, ack);
    CMD_POST(vname, "Update", ret);
}

grpc::Status SnapshotMgr::CowOp(ServerContext* context, const CowReq* req,
                                CowAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Cowop");
    CMD_DO(vname, cow_op, req, ack);
    CMD_POST(vname, "Cowop", ret);
}

grpc::Status SnapshotMgr::CowUpdate(ServerContext* context,
            const CowUpdateReq* req, CowUpdateAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "CowUpdate");
    CMD_DO(vname, cow_update, req, ack);
    CMD_POST(vname, "CowUpdate", ret);
}

grpc::Status SnapshotMgr::Diff(ServerContext* context, const DiffReq* req,
                               DiffAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Diff");
    CMD_DO(vname, diff_snapshot, req, ack);
    CMD_POST(vname, "Diff", ret);
}

grpc::Status SnapshotMgr::Read(ServerContext* context, const ReadReq* req,
                               ReadAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Read");
    CMD_DO(vname, read_snapshot, req, ack);
    CMD_POST(vname, "Read", ret);
}
