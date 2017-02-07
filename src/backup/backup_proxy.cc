#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <future>
#include "../log/log.h"
#include "../rpc/backup.pb.h"
#include "backup_proxy.h"

using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;

BackupProxy::BackupProxy(const string& vol_name, const size_t& vol_size, 
                         shared_ptr<SnapshotProxy> snap_proxy)
{
    m_vol_name = vol_name;
    m_vol_size = vol_size;
    m_snap_proxy = snap_proxy;
}

BackupProxy::~BackupProxy()
{
}

StatusCode BackupProxy::create_backup(const CreateBackupReq* req, CreateBackupAck* ack)
{
    string vol_name    = req->vol_name();
    string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_option().backup_mode();

    LOG_INFO << "create backup vname:" << " bname:" << backup_name;
    //todo
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "create backup vname:" << vol_name <<" bname:" << backup_name <<" ok";
    return StatusCode::sOk; 
}

StatusCode BackupProxy::list_backup(const ListBackupReq* req, ListBackupAck* ack)
{
    string vol_name = req->vol_name();
    LOG_INFO << "list backup vname:" << vol_name;
    //todo
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "list backup vname:" << vol_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupProxy::query_backup(const QueryBackupReq* req, QueryBackupAck* ack)
{
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "query backup vname:" << vol_name << " bname:" << backup_name;
    //todo
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "query backup vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupProxy::delete_backup(const DeleteBackupReq* req, DeleteBackupAck* ack)
{
    string vol_name    = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name;
    //todo
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupProxy::restore_backup(const RestoreBackupReq* req, RestoreBackupAck* ack)
{
    return StatusCode::sOk;
}
