#include "control_service.h"

ControlSvc* ControlSvc::s_instance = nullptr;
mutex* ControlSvc::s_mutex = new mutex;

void ControlSvc::start()
{
    /*todo: read configure file*/
    m_rpc_addr = "0.0.0.0:1111";
    m_rpc_builder.reset(new ServerBuilder);
    m_rpc_builder->AddListeningPort(m_rpc_addr, grpc::InsecureServerCredentials());
    m_rpc_builder->RegisterService(this);
    m_rpc_server = m_rpc_builder->BuildAndStart();
    if(m_rpc_server == nullptr){
        LOG_ERROR << "ControlSvc start failed m_rpc_server==nullptr" ;
        return;
    }

    m_work_thr.reset(new thread(bind(&ControlSvc::run, this)));
    LOG_INFO << "ControlSvc start";
}

void ControlSvc::run()
{
    LOG_INFO << "ControlSvc run";
    m_rpc_server->Wait();
    LOG_INFO << "ControlSvc run over";
}

void ControlSvc::stop()
{
    LOG_INFO << "ControlSvc stop";
    if(m_rpc_server){
        m_rpc_server->Shutdown(); 
    }
}

shared_ptr<SnapshotProxy> ControlSvc::get_vol_snap_proxy(const string& vol_name)
{
    auto it = m_volumes.find(vol_name);
    if(it != m_volumes.end()){
        return it->second->get_snapshot_proxy();
    }
    LOG_ERROR << "get_vol_snap_proxy vid:" << vol_name << "failed";
    return nullptr; 
}

Status ControlSvc:: CreateSnapshot(ServerContext*          context, 
                                  const CreateSnapshotReq* req, 
                                  CreateSnapshotAck*       ack) 
{
    /*find volume*/
    string vname = req->vol_name();
    LOG_INFO << "RPC ControlSvc CreateSnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    /*dispatch to volume*/
    vol_snap_proxy->create_snapshot(req, ack);
    LOG_INFO << "RPC ControlSvc CreateSnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status ControlSvc::ListSnapshot(ServerContext*    context, 
                               const ListSnapshotReq* req, 
                               ListSnapshotAck*       ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC ControlSvc ListSnapshot vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->list_snapshot(req, ack);
    LOG_INFO << "RPC ControlSvc ListSnapshot vname:" << vname << " ok";
    return Status::OK;
}

Status ControlSvc::DeleteSnapshot(ServerContext*           context, 
                                  const DeleteSnapshotReq* req, 
                                  DeleteSnapshotAck*       ack)
{    
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "RPC ControlSvc DeleteSnapshot"
             << " vname:" << vname
             << " sname:" << sname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->delete_snapshot(req, ack);

    LOG_INFO << "RPC ControlSvc DeleteSnapshot"
             << " vname:" << vname 
             << " sname:" << sname << " ok";
    return Status::OK;
}

Status ControlSvc::RollbackSnapshot(ServerContext*             context, 
                                    const RollbackSnapshotReq* req, 
                                    RollbackSnapshotAck*       ack) 
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "RPC ControlSvc RollbackSnapshot"
             << " vname:" << vname
             << " sname:" << sname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->rollback_snapshot(req, ack);

    LOG_INFO << "RPC ControlSvc RollbackSnapshot"
             << " vname:" << vname 
             << " sname:" << sname << " ok";
    return Status::OK;
}

 Status ControlSvc::DiffSnapshot(ServerContext*         context, 
                                 const DiffSnapshotReq* req, 
                                 DiffSnapshotAck*       ack)
{
    string vname = req->vol_name();
    string sname1 = req->first_snap_name();
    string sname2 = req->last_snap_name();
    LOG_INFO << "RPC ControlSvc DiffSnapshot"
             << " vname:" << vname
             << " sname1:" << sname1
             << " sname2:" << sname2;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->diff_snapshot(req, ack);

    LOG_INFO << "RPC ControlSvc DiffSnapshot"
             << " vname:" << vname
             << " sname1:" << sname1
             << " sname2:" << sname2
             << " ok"; 
    return Status::OK;
}

Status ControlSvc::ReadSnapshot(ServerContext*         context, 
                                const ReadSnapshotReq* req, 
                                ReadSnapshotAck*       ack)
{
    string vname = req->vol_name();
    string sname = req->snap_name();
    off_t  off   = req->off();
    size_t len   = req->len();
    LOG_INFO << "RPC ControlSvc ReadSnapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " off:" << off
             << " len:" << len;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    vol_snap_proxy->read_snapshot(req, ack);

    LOG_INFO << "RPC ControlSvc ReadSnapshot"
             << " vname:" << vname
             << " sname:" << sname
             << " off:" << off
             << " len:" << len
             << " ok"; 
    return Status::OK;
}
