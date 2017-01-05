#include "control_service.h"

shared_ptr<SnapshotProxy> ControlService::get_vol_snap_proxy(const string& vol_name)
{
    auto it = m_volumes.find(vol_name);
    if(it != m_volumes.end()){
        return it->second->get_snapshot_proxy();
    }
    LOG_ERROR << "get_vol_snap_proxy vid:" << vol_name << "failed";
    return nullptr; 
}

Status ControlService::CreateSnapshot(ServerContext* context, 
                                      const CreateSnapshotReq* req, 
                                      CreateSnapshotAck* ack) 
{
    /*find volume*/
    string vname = req->vol_name();
    LOG_INFO << "RPC ControlService CreateSnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    /*dispatch to volume*/
    vol_snap_proxy->create_snapshot(req, ack);
    LOG_INFO << "RPC ControlService CreateSnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status ControlService::ListSnapshot(ServerContext* context, 
                                    const ListSnapshotReq* req, 
                                    ListSnapshotAck* ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC ControlService ListSnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->list_snapshot(req, ack);
    LOG_INFO << "RPC ControlService ListSnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status ControlService::DeleteSnapshot(ServerContext* context, 
                                      const DeleteSnapshotReq* req, 
                                      DeleteSnapshotAck* ack)
{    
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "RPC ControlService DeleteSnapshot"
             << " vname:" << vname
             << " sname:" << sname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->delete_snapshot(req, ack);

    LOG_INFO << "RPC ControlService DeleteSnapshot"
             << " vname:" << vname 
             << " sname:" << sname << " ok";
    return Status::OK;
}

Status ControlService::RollbackSnapshot(ServerContext* context, 
                                        const RollbackSnapshotReq* req, 
                                        RollbackSnapshotAck* ack) 
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "RPC ControlService RollbackSnapshot"
             << " vname:" << vname
             << " sname:" << sname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->rollback_snapshot(req, ack);

    LOG_INFO << "RPC ControlService RollbackSnapshot"
             << " vname:" << vname 
             << " sname:" << sname << " ok";
    return Status::OK;
}

 Status ControlService::DiffSnapshot(ServerContext* context, 
                                     const DiffSnapshotReq* req, 
                                     DiffSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname1 = req->first_snap_name();
    string sname2 = req->last_snap_name();
    LOG_INFO << "RPC ControlService DiffSnapshot"
             << " vname:" << vname
             << " sname1:" << sname1
             << " sname2:" << sname2;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->diff_snapshot(req, ack);

    LOG_INFO << "RPC ControlService DiffSnapshot"
             << " vname:" << vname
             << " sname1:" << sname1
             << " sname2:" << sname2
             << " ok"; 
    return Status::OK;
}

Status ControlService::ReadSnapshot(ServerContext* context, 
                                    const ReadSnapshotReq* req, 
                                    ReadSnapshotAck* ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    off_t  off   = req->off();
    size_t len   = req->len();
    LOG_INFO << "RPC ControlService ReadSnapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " off:" << off
             << " len:" << len;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->read_snapshot(req, ack);

    LOG_INFO << "RPC ControlService ReadSnapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " off:" << off
             << " len:" << len
             << " ok"; 
    return Status::OK;
}
