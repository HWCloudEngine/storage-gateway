#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "../log/log.h"
#include "snapshot_mgr.h"

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
        shared_ptr<SnapshotMds> volume;
        volume.reset(new SnapshotMds(vname));
        m_volumes.insert(pair<string, shared_ptr<SnapshotMds>>(vname, volume));
        /*snapshot mds recover*/
        volume->recover();
    }

    closedir(pdir);
}

void SnapshotMgr::fini()
{
    m_volumes.clear();
}

grpc::Status SnapshotMgr::Sync(ServerContext* context, 
                               const SyncReq* req, 
                               SyncAck*       ack)
{
    string vname = req->vol_name();
    LOG_INFO << "SnapshotMgr sync" << " vname:" << vname;

    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr sync failed vname:" << vname;
        return grpc::Status::CANCELLED;
    }

    int ret = it->second->sync(req, ack);
    if(ret){
        LOG_INFO << "SnapshotMgr sync" << " vname:" << vname << " failed";
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr sync" << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr:: Create(ServerContext*   context, 
                                  const CreateReq* req, 
                                  CreateAck*       ack) 
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    shared_ptr<SnapshotMds> volume;

    if(it != m_volumes.end()){
        volume = it->second;
        goto create;
    }
    
    /*create snapshotmds for each volume*/
    volume.reset(new SnapshotMds(vname));
    m_volumes.insert(pair<string, shared_ptr<SnapshotMds>>(vname, volume));

create:
    LOG_INFO << "SnapshotMgr create" << " vname:" << vname;
    int ret = volume->create_snapshot(req, ack);
    if(ret){
        LOG_INFO << "SnapshotMgr create" << " vname:" << vname << " failed";
        return grpc::Status::CANCELLED;
    }

    LOG_INFO << "SnapshotMgr create" << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::List(ServerContext* context, 
                               const ListReq* req, 
                               ListAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr list failed vname:" << vname;
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr list" << " vname:" << vname;
    int ret = it->second->list_snapshot(req, ack);
    if(ret){
        LOG_INFO << "SnapshotMgr list" << " vname:" << vname << " failed";
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr list" << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::Delete(ServerContext*   context, 
                                 const DeleteReq* req, 
                                 DeleteAck*       ack)
{
    string vname = req->vol_name();
    string snap_name = req->snap_name();

    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr delete failed vname:" << vname;
        return grpc::Status::CANCELLED;
    }

    LOG_INFO << "SnapshotMgr delete" << " vname:" << vname 
             << " snap_name:" << snap_name;

    int ret = it->second->delete_snapshot(req, ack);
    if(ret){
        LOG_INFO << "SnapshotMgr delete" << " vname:" << vname
                 << " snap_name:" << snap_name << " failed";
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr delete" << " vname:" << vname
             << " snap_name:" << snap_name << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::Rollback(ServerContext*     context, 
                                   const RollbackReq* req, 
                                   RollbackAck*       ack) 
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr Rollback" << " vname:" << vname;
    int ret = it->second->rollback_snapshot(req, ack);
    if(ret){
        LOG_INFO << "SnapshotMgr Rollback" << " vname:" << vname << " failed";
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr Rollback" << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::Update(ServerContext* context,
                                 const UpdateReq* req,
                                 UpdateAck* ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr update" << " vname:" << vname;
    int ret = it->second->update(req, ack);
    if(ret){
        LOG_INFO << "SnapshotMgr update" << " vname:" << vname << " failed";
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr update" << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::CowOp(ServerContext* context,
                                const CowReq*  req,
                                CowAck*        ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr Cow" << " vname:" << vname;
    int ret = it->second->cow_op(req, ack);
    LOG_INFO << "SnapshotMgr Cow" << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::CowUpdate(ServerContext* context,
                                    const CowUpdateReq* req,
                                    CowUpdateAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr CowUpdate vname:" << vname;
    int ret = it->second->cow_update(req, ack);
    LOG_INFO << "SnapshotMgr CowUpdate vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::Diff(ServerContext* context, 
                               const DiffReq* req, 
                               DiffAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr Diff vname:" << vname;
    int ret = it->second->diff_snapshot(req, ack);
    LOG_INFO << "SnapshotMgr Diff vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::Read(ServerContext* context, 
                               const ReadReq* req, 
                               ReadAck*      ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr Read vname:" << vname;
    int ret = it->second->read_snapshot(req, ack);
    LOG_INFO << "SnapshotMgr Read vname:" << vname << " ok";
    return grpc::Status::OK;
}

