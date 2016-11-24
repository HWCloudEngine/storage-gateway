#include "snapshot_mgr.h"

grpc::Status SnapshotMgr::GetUid(ServerContext*   context,
                                 const GetUidReq* req,
                                 GetUidAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    int ret = 0;
    shared_ptr<SnapshotMds> volume;
    if(it != m_volumes.end()){
        volume = it->second;
        ret = volume->get_uid(req, ack);
        return grpc::Status::OK;
    }

    volume.reset(new SnapshotMds(vname));
    m_volumes.insert(pair<string, shared_ptr<SnapshotMds>>(vname, volume));
    LOG_INFO << "SnapshotMgr get uid"
             << " vname:" << vname;
    ret = volume->get_uid(req, ack);
    LOG_INFO << "SnapshotMgr get uid"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::GetSnapshotName(ServerContext*            context,
                                          const GetSnapshotNameReq* req,
                                          GetSnapshotNameAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr get snapshot name failed vanem:" << vname;
        return grpc::Status::CANCELLED;
    }

    LOG_INFO << "SnapshotMgr get snapshot name"
             << " vname:" << vname;
    int ret = it->second->get_snapshot_name(req, ack);
    LOG_INFO << "SnapshotMgr get snapshot name"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::GetSnapshotId(ServerContext*          context,
                                        const GetSnapshotIdReq* req,
                                        GetSnapshotIdAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr get snapshot id failed vanem:" << vname;
        return grpc::Status::CANCELLED;
    }

    LOG_INFO << "SnapshotMgr get snapshot id"
             << " vname:" << vname;
    int ret = it->second->get_snapshot_id(req, ack);
    LOG_INFO << "SnapshotMgr get snapshot id"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr:: Create(ServerContext*   context, 
                                  const CreateReq* req, 
                                  CreateAck*       ack) 
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr create failed vanem:" << vname;
        return grpc::Status::CANCELLED;
    }

    LOG_INFO << "SnapshotMgr create"
             << " vname:" << vname;
    int ret = it->second->create_snapshot(req, ack);
    LOG_INFO << "SnapshotMgr create"
             << " vname:" << vname << " ok";
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
    LOG_INFO << "SnapshotMgr list"
             << " vname:" << vname;
    int ret = it->second->list_snapshot(req, ack);
    LOG_INFO << "SnapshotMgr list"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::Delete(ServerContext*   context, 
                                 const DeleteReq* req, 
                                 DeleteAck*       ack)
{
    string vname  = req->vol_name();
    snapid_t snap_id = req->snap_id();

    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        LOG_ERROR << "SnapshotMgr delete failed vname:" << vname;
        return grpc::Status::CANCELLED;
    }

    LOG_INFO << "SnapshotMgr delete"
             << " vname:" << vname
             << " snap_id:" << snap_id;

    int ret = it->second->delete_snapshot(req, ack);

    LOG_INFO << "SnapshotMgr delete"
             << " vname:" << vname
             << " snap_id:" << snap_id << " ok";
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
    LOG_INFO << "SnapshotMgr Rollback"
             << " vname:" << vname;
    int ret = it->second->rollback_snapshot(req, ack);
    LOG_INFO << "SnapshotMgr Rollback"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}


grpc::Status SnapshotMgr::SetStatus(ServerContext*      context,
                                    const SetStatusReq* req,
                                    SetStatusAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr setstatus"
             << " vname:" << vname;
    int ret = it->second->set_status(req, ack);
    LOG_INFO << "SnapshotMgr setstatus"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::GetStatus(ServerContext*      context,
                                    const GetStatusReq* req,
                                    GetStatusAck*       ack)
{
    string vname = req->vol_name();
    auto it = m_volumes.find(vname);
    if(it == m_volumes.end()){
        return grpc::Status::CANCELLED;
    }
    LOG_INFO << "SnapshotMgr getstatus"
             << " vname:" << vname;
    int ret = it->second->get_status(req, ack);
    LOG_INFO << "SnapshotMgr getstatus"
             << " vname:" << vname << " ok";
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
    LOG_INFO << "SnapshotMgr Cow"
             << " vname:" << vname;
    int ret = it->second->cow_op(req, ack);
    LOG_INFO << "SnapshotMgr Cow"
             << " vname:" << vname << " ok";
    return grpc::Status::OK;
}

grpc::Status SnapshotMgr::CowUpdate(ServerContext*      context,
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

