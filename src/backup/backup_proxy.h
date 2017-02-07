#ifndef _BACKUP_PROXY_H
#define _BACKUP_PROXY_H
#include <string>
#include <memory>
#include "../rpc/common.pb.h"
#include "../rpc/backup_control.pb.h"
#include "../rpc/backup_control.grpc.pb.h"
#include "../common/block_store.h"
#include "backup_decorator.h"

using namespace std;

using huawei::proto::StatusCode;
using huawei::proto::control::CreateBackupReq;
using huawei::proto::control::CreateBackupAck;
using huawei::proto::control::ListBackupReq;
using huawei::proto::control::ListBackupAck;
using huawei::proto::control::GetBackupReq;
using huawei::proto::control::GetBackupAck;
using huawei::proto::control::DeleteBackupReq;
using huawei::proto::control::DeleteBackupAck;
using huawei::proto::control::RestoreBackupReq;
using huawei::proto::control::RestoreBackupAck;

class BackupProxy 
{
public:
    BackupProxy(const string& vol_name, const size_t& vol_size, shared_ptr<BackupDecorator> backup_decorator);
    virtual ~BackupProxy();   

    /*snapshot common operation*/
    StatusCode create_backup(const CreateBackupReq* req, CreateBackupAck* ack);
    StatusCode delete_backup(const DeleteBackupReq* req, DeleteBackupAck* ack);
    StatusCode restore_backup(const RestoreBackupReq* req, RestoreBackupAck* ack);
    StatusCode list_backup(const ListBackupReq* req, ListBackupAck* ack);
    StatusCode get_backup(const GetBackupReq* req, GetBackupAck* ack);
    
    /*crash recover*/
    int recover();

private:
    /*volume basic*/
    string m_vol_name;
    size_t m_vol_size;
    
    shared_ptr<BackupDecorator>       m_backup_decorator;
    shared_ptr<BackupInnerCtrlClient> m_backup_inner_rpc_client;
    shared_ptr<BlockStore>            m_block_store;
};

#endif
