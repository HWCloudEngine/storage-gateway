#include "log/log.h"
#include "rpc/backup.pb.h"
#include "common/utils.h"
#include "common/define.h"
#include "backup_proxy.h"

using huawei::proto::SnapScene;
using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;

BackupProxy::BackupProxy(VolumeAttr& vol_attr, shared_ptr<BackupDecorator> backup_decorator)
    :m_vol_attr(vol_attr), m_backup_decorator(backup_decorator)
{
    /*todo read from config*/
    m_backup_inner_rpc_client.reset(new BackupInnerCtrlClient(grpc::CreateChannel(
                                    "127.0.0.1:50051", 
                                    grpc::InsecureChannelCredentials())));
    m_block_store.reset(new CephBlockStore());
}

BackupProxy::~BackupProxy()
{
    m_backup_inner_rpc_client.reset();
    m_block_store.reset();
}

StatusCode BackupProxy::create_backup(const CreateBackupReq* req, CreateBackupAck* ack)
{
    string vol_name = req->vol_name();
    size_t vol_size = req->vol_size();
    string backup_name = req->backup_name();
    BackupOption backup_option = req->backup_option();
    BackupMode backup_mode = backup_option.backup_mode();
    BackupType backup_type = backup_option.backup_type();
    StatusCode ret = StatusCode::sOk;

    LOG_INFO << "create backup vname:" << vol_name << " bname:" << backup_name;

    do {
        if(!m_vol_attr.is_backup_allowable(backup_type)){
            LOG_ERROR << "create backup vname:" << vol_name << " bname:" 
                      << backup_name << " disallow";
            ret = StatusCode::sBackupCreateDenied;
            break;
        }

        // transaction begin
        m_backup_decorator->add_sync(backup_name, "backup on creating");

        //1. create snapshot
        string snap_name = backup_to_snap_name(backup_name);
        CreateSnapshotReq snap_req;
        CreateSnapshotAck snap_ack;
        snap_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
        snap_req.set_vol_name(vol_name);
        snap_req.set_vol_size(vol_size);
        snap_req.set_snap_name(snap_name);
        ret = m_backup_decorator->create_snapshot(&snap_req, &snap_ack);
        if(ret != StatusCode::sOk){
            LOG_ERROR << "create backup vname:" << vol_name << " bname:" << backup_name 
                      << "create snapshot failed:" << ret;
            break;
        }

        //2.create backup
        ret = m_backup_inner_rpc_client->CreateBackup(vol_name, vol_size, 
                                                      backup_name, backup_option);
        if(ret != StatusCode::sOk){
            LOG_ERROR << "create backup vname:" << vol_name << " bname:" << backup_name 
                      << "create backup failed:" << ret;
            break;
        }

    }while(0);

    //transaction end
    m_backup_decorator->del_sync(backup_name);
    ack->set_status(ret);
    LOG_INFO << "create backup vname:" << vol_name <<" bname:" << backup_name 
             << (ret ? " ok" : "failed");
    return ret; 
}

StatusCode BackupProxy::list_backup(const ListBackupReq* req, ListBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    set<string> backup_set;
    LOG_INFO << "list backup vname:" << vol_name;

    do {
        ret = m_backup_inner_rpc_client->ListBackup(vol_name, backup_set);
        if(ret != StatusCode::sOk){
            break;
        }
        for(auto backup : backup_set){
            string* add_backup_name = ack->add_backup_name();
            add_backup_name->copy(const_cast<char*>(backup.c_str()), backup.length());
        }
    }while(0);

    ack->set_status(ret);
    LOG_INFO << "list backup vname:" << vol_name << (ret ? " ok" : " failed");
    return ret;
}

StatusCode BackupProxy::get_backup(const GetBackupReq* req, GetBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "get backup vname:" << vol_name << " bname:" << backup_name;
    BackupStatus backup_status;

    do {
        ret = m_backup_inner_rpc_client->GetBackup(vol_name, backup_name, backup_status);
        if(ret != StatusCode::sOk){
            break;
        }
        ack->set_backup_status(backup_status);
    }while(0);

    ack->set_status(ret);
    LOG_INFO << "get backup vname:" << vol_name << (ret ? " ok" : " failed");
    return ret;
}

StatusCode BackupProxy::delete_backup(const DeleteBackupReq* req, DeleteBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name;
    ret = m_backup_inner_rpc_client->DeleteBackup(vol_name, backup_name);
    ack->set_status(ret);
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name 
             << (ret ? " ok" : " failed");
    return ret; 
}

StatusCode BackupProxy::restore_backup(const RestoreBackupReq* req, RestoreBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    string new_vol_name = req->new_vol_name();
    size_t new_vol_size = req->new_vol_size();
    string new_block_device = req->new_block_device();
    
    LOG_INFO << "restore backup vname:" << vol_name << " bname:" << backup_name;
    ret = m_backup_inner_rpc_client->RestoreBackup(vol_name, backup_name, 
                                                   new_vol_name, 
                                                   new_vol_size, 
                                                   new_block_device,
                                                   m_block_store.get());
    ack->set_status(ret);
    LOG_INFO << "restore backup vname:" << vol_name << " bname:" << backup_name 
             << (ret ? " ok" : " failed");
    return ret;
}
