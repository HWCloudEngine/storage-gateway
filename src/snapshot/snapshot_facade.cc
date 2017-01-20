#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "../log/log.h"
#include "snapshot_facade.h"

using huawei::proto::StatusCode;
using huawei::proto::SnapScene;

#define CMD_PREV(vol, op)            \
do {                                 \
    string log_msg = "SnapshotFacade";  \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    LOG_INFO << log_msg;             \
}while(0)

#define CMD_POST(vol, op, ret)       \
do {                                 \
    string log_msg = "SnapshotFacade";  \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    if(!ret) {                          \
        log_msg += " ok";               \
        LOG_INFO << log_msg;            \
        return grpc::Status::OK;        \
    } else {                            \
        log_msg += " failed";           \
        LOG_INFO << log_msg;            \
        return grpc::Status::CANCELLED; \
    }                                   \
}while(0)

#define CMD_DO(op, req, ack)       \
do {                               \
    switch(req->header().scene())  \
    {                              \
    case SnapScene::FOR_NORMAL:         \
        ret = m_snap_mds->op(req, ack); \
        break;                          \
    case SnapScene::FOR_REPLICATION:    \
    case SnapScene::FOR_BACKUP:         \
        break;                          \
    default:                            \
        break;                          \
    }                                   \
}while(0)

SnapshotFacade::SnapshotFacade(const string vol_name)
{
    m_vol_name = vol_name;
    m_snap_mds = new SnapshotMds(vol_name);
}

SnapshotFacade::~SnapshotFacade()
{
    delete m_snap_mds;
}

int SnapshotFacade::recover()
{
    m_snap_mds->recover();
    return 0;
}

grpc::Status SnapshotFacade::Sync(const SyncReq* req, SyncAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "sync");
    CMD_DO(sync, req, ack);
    CMD_POST(vname, "sync", ret);
}

grpc::Status SnapshotFacade::Create(const CreateReq* req, CreateAck* ack) 
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "create");
    CMD_DO(create_snapshot, req, ack);
    CMD_POST(vname, "create", ret);
}

grpc::Status SnapshotFacade::List(const ListReq* req, ListAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "list");
    CMD_DO(list_snapshot, req, ack);
    CMD_POST(vname, "list", ret);
}

grpc::Status SnapshotFacade::Query(const QueryReq* req, QueryAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "query");
    CMD_DO(query_snapshot, req, ack);
    CMD_POST(vname, "query", ret);
}

grpc::Status SnapshotFacade::Delete(const DeleteReq* req, DeleteAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "delete");
    CMD_DO(delete_snapshot, req, ack);
    CMD_POST(vname, "delete", ret);
}

grpc::Status SnapshotFacade::Rollback(const RollbackReq* req, RollbackAck* ack) 
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "rollback");
    CMD_DO(rollback_snapshot, req, ack);
    CMD_POST(vname, "rollback", ret);
}

grpc::Status SnapshotFacade::Update(const UpdateReq* req, UpdateAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "update");
    CMD_DO(update, req, ack);
    CMD_POST(vname, "update", ret);
}

grpc::Status SnapshotFacade::CowOp(const CowReq*  req, CowAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "cowop");
    CMD_DO(cow_op, req, ack);
    CMD_POST(vname, "cowop", ret);
}

grpc::Status SnapshotFacade::CowUpdate(const CowUpdateReq* req, CowUpdateAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "cowupdate");
    CMD_DO(cow_update, req, ack);
    CMD_POST(vname, "cowupdate", ret);
}

grpc::Status SnapshotFacade::Diff(const DiffReq* req, DiffAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();
    
    CMD_PREV(vname, "diff");
    CMD_DO(diff_snapshot, req, ack);
    CMD_POST(vname, "diff", ret);
}

grpc::Status SnapshotFacade::Read(const ReadReq* req, ReadAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "read");
    CMD_DO(read_snapshot, req, ack);
    CMD_POST(vname, "read", ret);
}
