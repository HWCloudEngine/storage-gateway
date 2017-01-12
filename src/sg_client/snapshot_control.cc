#include "snapshot_control.h"

using huawei::proto::StatusCode;

shared_ptr<SnapshotProxy> SnapshotControlImpl::get_vol_snap_proxy(const string& vol_name)
{
    auto it = m_volumes.find(vol_name);
    if(it != m_volumes.end()){
        return it->second->get_snapshot_proxy();
    }
    LOG_ERROR << "get_vol_snap_proxy vid:" << vol_name << "failed";
    return nullptr; 
}

Status SnapshotControlImpl::CreateSnapshot(ServerContext* context, 
                                           const CreateSnapshotReq* req, 
                                           CreateSnapshotAck* ack) 
{
    /*todo: control should make sense of volume status 
     *  master volume:
     *       can create local and remote snapshot 
     *  slave volume: 
     *       can create remote snapshot only 
     */

    /*find volume*/
    string vname = req->vol_name();
    LOG_INFO << "RPC CreateSnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->create_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC CreateSnapshot vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }
    LOG_INFO << "RPC CreateSnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::ListSnapshot(ServerContext* context, 
                                         const ListSnapshotReq* req, 
                                         ListSnapshotAck* ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC ListSnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->list_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC ListSnapshot vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC ListSnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::QuerySnapshot(ServerContext* context, 
                                          const QuerySnapshotReq* req, 
                                          QuerySnapshotAck* ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC QuerySnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->query_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC QuerySnapshot vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC QuerySnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::DeleteSnapshot(ServerContext* context, 
                                           const DeleteSnapshotReq* req, 
                                           DeleteSnapshotAck* ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC DeleteSnapshot" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->delete_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC DeleteSnapshot vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC DeleteSnapshot" << " vname:" << vname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::RollbackSnapshot(ServerContext* context, 
                                             const RollbackSnapshotReq* req, 
                                             RollbackSnapshotAck* ack) 
{
    string vname = req->vol_name();
    LOG_INFO << "RPC RollbackSnapshot" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->rollback_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC RollbackSnapshot vname:" 
                 << vname << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC RollbackSnapshot" << " vname:" << vname << " ok";
    return Status::OK;
}

 Status SnapshotControlImpl::DiffSnapshot(ServerContext* context, 
                                          const DiffSnapshotReq* req, 
                                          DiffSnapshotAck* ack)
{
    string vname = req->vol_name();
    LOG_INFO << "RPC DiffSnapshot" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->diff_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC DiffSnapshot vname:" << vname  
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC DiffSnapshot vname:" << vname << " ok"; 
    return Status::OK;
}

Status SnapshotControlImpl::ReadSnapshot(ServerContext* context, 
                                         const ReadSnapshotReq* req, 
                                         ReadSnapshotAck* ack)
{
    string vname = req->vol_name();
    LOG_INFO << "RPC ReadSnapshot" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->read_snapshot(req, ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC ReadSnapshot vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC ReadSnapshot vname:" << vname << " ok"; 
    return Status::OK;
}
