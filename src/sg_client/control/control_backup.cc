#include "../../rpc/snapshot.pb.h"
#include "../../rpc/backup.pb.h"
#include "../../rpc/snapshot_control.pb.h"
#include "../../snapshot/backup_util.h"

#include "control_backup.h"

using huawei::proto::StatusCode;
using huawei::proto::BackupOption;
using huawei::proto::SnapScene;

using huawei::proto::control::CreateSnapshotReq;
using huawei::proto::control::CreateSnapshotAck;
using huawei::proto::control::ListSnapshotReq;
using huawei::proto::control::ListSnapshotAck;
using huawei::proto::control::QuerySnapshotReq;
using huawei::proto::control::QuerySnapshotAck;
using huawei::proto::control::RollbackSnapshotReq;
using huawei::proto::control::RollbackSnapshotAck;
using huawei::proto::control::DeleteSnapshotReq;
using huawei::proto::control::DeleteSnapshotAck;

shared_ptr<SnapshotProxy> BackupControlImpl::get_vol_snap_proxy(const string& vol_name)
{
    auto it = m_volumes.find(vol_name);
    if(it != m_volumes.end()){
        return it->second->get_snapshot_proxy();
    }
    LOG_ERROR << "get_vol_snap_proxy vid:" << vol_name << "failed";
    return nullptr; 
}

Status BackupControlImpl::CreateBackup(ServerContext* context, 
                                       const CreateBackupReq* req, 
                                       CreateBackupAck* ack) 
{
    /*find volume*/
    string vname = req->vol_name();
    LOG_INFO << "RPC CreateBackup vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    /*dispatch to volume*/
    
    /*backup convert to snapshot*/
    string backup_name = req->backup_name();
    string snap_name   = backup_to_snap_name(backup_name);

    CreateSnapshotReq new_req;
    new_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
    new_req.set_vol_name(vname);
    new_req.set_snap_name(snap_name);
    new_req.set_backup_name(backup_name);
    BackupOption* op = new_req.mutable_backup_option();
    op->CopyFrom(req->backup_option());
    CreateSnapshotAck new_ack;
    
    StatusCode ret = vol_snap_proxy->create_snapshot(&new_req, &new_ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC CreateBackup vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }
    LOG_INFO << "RPC CreateBackup vname:" << vname << " ok";
    return Status::OK;
}


Status BackupControlImpl::QueryBackup(ServerContext* context, 
                                      const QueryBackupReq* req, 
                                      QueryBackupAck* ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC QueryBackup vname:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    string backup_name = req->backup_name();
    string snap_name   = backup_to_snap_name(backup_name);

    QuerySnapshotReq new_req;
    QuerySnapshotAck new_ack;
    new_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
    new_req.set_vol_name(vname);
    StatusCode ret = vol_snap_proxy->query_snapshot(&new_req, &new_ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC QueryBackup vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC QueryBackup vname:" << vname << " ok";
    return Status::OK;
}

Status BackupControlImpl::DeleteBackup(ServerContext* context, 
                                       const DeleteBackupReq* req, 
                                       DeleteBackupAck* ack)
{    
    string vname = req->vol_name();
    LOG_INFO << "RPC DeleteBackup" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    DeleteSnapshotReq new_req;
    DeleteSnapshotAck new_ack;
    new_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
    new_req.set_vol_name(vname);
    StatusCode ret = vol_snap_proxy->delete_snapshot(&new_req, &new_ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC DeleteBackup vname:" << vname 
                  << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC DeleteBackup" << " vname:" << vname << " ok";
    return Status::OK;
}

Status BackupControlImpl::RestoreBackup(ServerContext* context, 
                                        const RestoreBackupReq* req, 
                                        RestoreBackupAck* ack) 
{
    string vname = req->vol_name();
    LOG_INFO << "RPC RestoreBackup" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    RollbackSnapshotReq new_req;
    RollbackSnapshotAck new_ack;
    new_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
    StatusCode ret = vol_snap_proxy->rollback_snapshot(&new_req, &new_ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC RestoreBackup vname:" 
                 << vname << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC RestoreBackup" << " vname:" << vname << " ok";
    return Status::OK;
}

Status BackupControlImpl::ListBackups(ServerContext* context, 
                                      const ListBackupsReq* req, 
                                      ListBackupsAck* ack) 
{
    string vname = req->vol_name();
    LOG_INFO << "RPC RestoreBackup" << " vname:" << vname;

    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    assert(vol_snap_proxy != nullptr);
    
    /*dispatch to volume*/
    ListSnapshotReq new_req;
    ListSnapshotAck new_ack;
    new_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
    StatusCode ret = vol_snap_proxy->list_snapshot(&new_req, &new_ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "RPC RestoreBackup vname:" 
                 << vname << " failed" << " err:" << ret;
        return Status::CANCELLED;
    }

    LOG_INFO << "RPC RestoreBackup" << " vname:" << vname << " ok";
    return Status::OK;
}
