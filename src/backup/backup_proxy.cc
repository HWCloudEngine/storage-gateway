#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <future>
#include "../log/log.h"
#include "../rpc/backup.pb.h"
#include "../common/utils.h"
#include "backup_type.h"
#include "backup_proxy.h"

using huawei::proto::SnapScene;
using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::RestoreBlocks;

BackupProxy::BackupProxy(const string& vol_name, const size_t& vol_size, 
                         shared_ptr<BackupDecorator> backup_decorator)
{
    m_vol_name = vol_name;
    m_vol_size = vol_size;
    m_backup_decorator = backup_decorator;

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
    string backup_name = req->backup_name();
    BackupOption backup_option = req->backup_option();
    BackupMode backup_mode = req->backup_option().backup_mode();
    StatusCode ret = StatusCode::sOk;

    LOG_INFO << "create backup vname:" << " bname:" << backup_name;
    // transaction begin
    m_backup_decorator->add_sync(backup_name, "backup on creating");

    //1. create snapshot
    string snap_name = backup_to_snap_name(backup_name);
    CreateSnapshotReq snap_req;
    CreateSnapshotAck snap_ack;
    snap_req.mutable_header()->set_scene(SnapScene::FOR_BACKUP);
    snap_req.set_vol_name(m_vol_name);
    snap_req.set_vol_size(m_vol_size);
    snap_req.set_snap_name(snap_name);
    ret = m_backup_decorator->create_snapshot(&snap_req, &snap_ack);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "create backup vname:" << vol_name << " bname:" << backup_name 
                  << "create snapshot failed:" << ret;
        ack->set_status(ret);
        return ret; 
    }

    //2.create backup
    ret = m_backup_inner_rpc_client->CreateBackup(m_vol_name, m_vol_size, 
                                                  backup_name, backup_option);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "create backup vname:" << vol_name << " bname:" << backup_name 
                  << "create backup failed:" << ret;
        ack->set_status(ret);
        return ret; 
    }
    
    //transaction end
    m_backup_decorator->del_sync(backup_name);
    ack->set_status(ret);
    LOG_INFO << "create backup vname:" << vol_name <<" bname:" << backup_name <<" ok";
    return ret; 
}

StatusCode BackupProxy::list_backup(const ListBackupReq* req, ListBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    set<string> backup_set;
    LOG_INFO << "list backup vname:" << vol_name;
    ret = m_backup_inner_rpc_client->ListBackup(vol_name, backup_set);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "list bakcup vname:" << vol_name << "failed:" << ret;
        ack->set_status(ret);
        return ret; 
    }
    for(auto backup : backup_set){
        string* add_backup_name = ack->add_backup_name();
        add_backup_name->copy(const_cast<char*>(backup.c_str()), backup.length());
    }
    ack->set_status(ret);
    LOG_INFO << "list backup vname:" << vol_name << " ok";
    return ret;
}

StatusCode BackupProxy::get_backup(const GetBackupReq* req, GetBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "get backup vname:" << vol_name << " bname:" << backup_name;
    BackupStatus backup_status;
    ret = m_backup_inner_rpc_client->GetBackup(vol_name, backup_name, backup_status);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "qet bakcup vname:" << vol_name << "failed:" << ret;
        ack->set_status(ret);
        return ret; 
    }
    ack->set_backup_status(backup_status);
    ack->set_status(ret);
    LOG_INFO << "get backup vname:" << vol_name << " ok";
    return ret;
}

StatusCode BackupProxy::delete_backup(const DeleteBackupReq* req, DeleteBackupAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name;
    ret = m_backup_inner_rpc_client->DeleteBackup(m_vol_name, backup_name);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "delete backup vname:" << vol_name << " bname:" << backup_name 
                  << "delete backup failed:" << ret;
        ack->set_status(ret);
        return ret; 
    }
    ack->set_status(ret);
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
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
    vector<RestoreBlocks> restore_blocks_vec;
    ret = m_backup_inner_rpc_client->RestoreBackup(vol_name, backup_name, 
                                                   new_vol_name, 
                                                   new_vol_size, 
                                                   new_block_device,
                                                   restore_blocks_vec);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "restore backup vname:" << vol_name << " bname:" << backup_name 
                  << "restore backup failed:" << ret;
        ack->set_status(ret);
        return ret; 
    }
    
    int new_block_device_fd = open(new_block_device.c_str(), O_RDWR | O_DIRECT | O_SYNC);
    assert(new_block_device_fd != -1);
    char* buf = (char*)malloc(BACKUP_BLOCK_SIZE);
    assert(buf != nullptr);

    for(auto restore_blocks : restore_blocks_vec){
        int block_num = restore_blocks.block_no_size();
        for(int i = 0; i < block_num; i++){
            uint64_t block_no = restore_blocks.block_no(i);
            string   block_obj = restore_blocks.block_obj(i);
            /*1. read from block store*/
            int read_ret = m_block_store->read(block_obj, buf, BACKUP_BLOCK_SIZE, 0);
            assert(read_ret == BACKUP_BLOCK_SIZE);

            /*2. (todo) whether should take snapshot before restore
             * in case of restore failed should rollback*/

            /*3. write to new block device*/
            int write_ret = pwrite(new_block_device_fd, buf, BACKUP_BLOCK_SIZE, 
                                   block_no * BACKUP_BLOCK_SIZE);
            assert(write_ret == BACKUP_BLOCK_SIZE);
        }
    }
    
    if(buf){
        free(buf); 
    }
    if(new_block_device_fd != -1){
        close(new_block_device_fd); 
    }

    ack->set_status(ret);
    LOG_INFO << "restore backup vname:" << vol_name << " bname:" << backup_name;
    return StatusCode::sOk;
}
