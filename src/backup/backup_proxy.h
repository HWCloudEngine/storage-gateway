#ifndef _BACKUP_PROXY_H
#define _BACKUP_PROXY_H
#include <string>
#include <memory>
#include "../rpc/common.pb.h"
#include "../rpc/backup_control.pb.h"
#include "../rpc/backup_control.grpc.pb.h"

#include "../snapshot/snapshot_proxy.h"

using namespace std;

using huawei::proto::StatusCode;
using huawei::proto::control::CreateBackupReq;
using huawei::proto::control::CreateBackupAck;
using huawei::proto::control::ListBackupReq;
using huawei::proto::control::ListBackupAck;
using huawei::proto::control::QueryBackupReq;
using huawei::proto::control::QueryBackupAck;
using huawei::proto::control::DeleteBackupReq;
using huawei::proto::control::DeleteBackupAck;
using huawei::proto::control::RestoreBackupReq;
using huawei::proto::control::RestoreBackupAck;

class BackupProxy 
{
public:
    BackupProxy(const string& vol_name, const size_t& vol_size, shared_ptr<SnapshotProxy> snap_proxy);
    virtual ~BackupProxy();   

    /*snapshot common operation*/
    StatusCode create_backup(const CreateBackupReq* req, CreateBackupAck* ack);
    StatusCode delete_backup(const DeleteBackupReq* req, DeleteBackupAck* ack);
    StatusCode restore_backup(const RestoreBackupReq* req, RestoreBackupAck* ack);
    StatusCode list_backup(const ListBackupReq* req, ListBackupAck* ack);
    StatusCode query_backup(const QueryBackupReq* req, QueryBackupAck* ack);
    
    /*crash recover*/
    int recover();

private:
    /*volume basic*/
    string m_vol_name;
    size_t m_vol_size;
    
    shared_ptr<SnapshotProxy> m_snap_proxy;
};

#endif
