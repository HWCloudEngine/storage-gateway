#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "../log/log.h"
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
    if(ret.ok()) {                      \
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
    auto it = m_snap_fecades.find(vname);      \
    if(it == m_snap_fecades.end()){            \
        ret = grpc::Status::CANCELLED; \
        break;                         \
    }                                  \
    ret = it->second->op(req, ack);    \
}while(0);


void SnapshotMgr::init()
{
    /*todo parallel optimize*/
    DIR* pdir = nullptr;
    struct dirent* pdentry = nullptr;
    pdir = opendir(DB_DIR);
    while((pdentry = readdir(pdir)) != nullptr){
        if(strcmp(pdentry->d_name, ".") == 0 || 
           strcmp(pdentry->d_name, "..") == 0){
            continue; 
        }
        /*get volume name*/
        string vname = pdentry->d_name;
        /*create snapshot mds for volume*/
        shared_ptr<SnapshotFecade> snap_fecade;
        snap_fecade.reset(new SnapshotFecade(vname));
        m_snap_fecades.insert({vname, snap_fecade});
        /*snapshot mds recover*/
        snap_fecade->recover();
    }

    closedir(pdir);
}

void SnapshotMgr::fini()
{
    m_snap_fecades.clear();
}

grpc::Status SnapshotMgr::Sync(ServerContext* context, 
                               const SyncReq* req, 
                               SyncAck*       ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Sync");
    CMD_DO(vname, Sync, req, ack);
    CMD_POST(vname, "Sync", ret);
}

grpc::Status SnapshotMgr::Create(ServerContext*   context, 
                                 const CreateReq* req, 
                                 CreateAck*       ack) 
{
    grpc::Status ret;
    string vname = req->vol_name();

    auto it = m_snap_fecades.find(vname);
    shared_ptr<SnapshotFecade> snap_fecade;
    if(it != m_snap_fecades.end()){
        snap_fecade = it->second;
        goto create;
    }
    
    /*create snapshotmds for each volume*/
    snap_fecade.reset(new SnapshotFecade(vname));
    m_snap_fecades.insert({vname, snap_fecade});

create:
    CMD_PREV(vname, "create");
    ret = snap_fecade->Create(req, ack);
    CMD_POST(vname, "create", ret);
}

grpc::Status SnapshotMgr::List(ServerContext* context, 
                               const ListReq* req, 
                               ListAck*       ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "List");
    CMD_DO(vname, List, req, ack);
    CMD_POST(vname, "List", ret);
}

grpc::Status SnapshotMgr::Query(ServerContext* context, 
                                const QueryReq* req, 
                                QueryAck*       ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Query");
    CMD_DO(vname, Query, req, ack);
    CMD_POST(vname, "Query", ret);
}

grpc::Status SnapshotMgr::Delete(ServerContext*   context, 
                                 const DeleteReq* req, 
                                 DeleteAck*       ack)
{
    grpc::Status ret;
    string vname = req->vol_name();
    string snap_name = req->snap_name();

    CMD_PREV(vname, "Delete");
    CMD_DO(vname, Delete, req, ack);
    CMD_POST(vname, "Delete", ret);
}

grpc::Status SnapshotMgr::Rollback(ServerContext*     context, 
                                   const RollbackReq* req, 
                                   RollbackAck*       ack) 
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Rollback");
    CMD_DO(vname, Rollback, req, ack);
    CMD_POST(vname, "Rollback", ret);
}

grpc::Status SnapshotMgr::Update(ServerContext* context,
                                 const UpdateReq* req,
                                 UpdateAck* ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Update");
    CMD_DO(vname, Update, req, ack);
    CMD_POST(vname, "Update", ret);
}

grpc::Status SnapshotMgr::CowOp(ServerContext* context,
                                const CowReq*  req,
                                CowAck*        ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Cowop");
    CMD_DO(vname, CowOp, req, ack);
    CMD_POST(vname, "Cowop", ret);
}

grpc::Status SnapshotMgr::CowUpdate(ServerContext* context,
                                    const CowUpdateReq* req,
                                    CowUpdateAck*       ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "CowUpdate");
    CMD_DO(vname, CowUpdate, req, ack);
    CMD_POST(vname, "CowUpdate", ret);
}

grpc::Status SnapshotMgr::Diff(ServerContext* context, 
                               const DiffReq* req, 
                               DiffAck*       ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Diff");
    CMD_DO(vname, Diff, req, ack);
    CMD_POST(vname, "Diff", ret);
}

grpc::Status SnapshotMgr::Read(ServerContext* context, 
                               const ReadReq* req, 
                               ReadAck*      ack)
{
    grpc::Status ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Read");
    CMD_DO(vname, Read, req, ack);
    CMD_POST(vname, "Read", ret);
}

