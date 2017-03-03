#include "../common/utils.h"
#include "backup_decorator.h"

BackupDecorator::BackupDecorator(string vol_name, 
                                 shared_ptr<SnapshotProxy> snapshot_proxy)
{
    m_vol_name = vol_name;
    m_snapshot_proxy = snapshot_proxy;
    m_backup_inner_rpc_client.reset(new BackupInnerCtrlClient(grpc::CreateChannel(
                                    "127.0.0.1:50051", 
                                    grpc::InsecureChannelCredentials())));
}

BackupDecorator::~BackupDecorator()
{
}

StatusCode BackupDecorator::create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack)
{
    return m_snapshot_proxy->create_snapshot(req, ack);
}

StatusCode BackupDecorator::delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack)
{
    return m_snapshot_proxy->delete_snapshot(req, ack);
}

StatusCode BackupDecorator::rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack)
{
    return m_snapshot_proxy->rollback_snapshot(req, ack);
}

StatusCode BackupDecorator::list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack)
{
    return m_snapshot_proxy->list_snapshot(req, ack);
}

StatusCode BackupDecorator::query_snapshot(const QuerySnapshotReq* req, QuerySnapshotAck* ack)
{
    return m_snapshot_proxy->query_snapshot(req, ack);
}

StatusCode BackupDecorator::diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack)
{
    return m_snapshot_proxy->diff_snapshot(req, ack);
}

StatusCode BackupDecorator::read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack)
{
    return m_snapshot_proxy->read_snapshot(req, ack);
}

StatusCode BackupDecorator::create_transaction(const SnapReqHead& shead, const string& snap_name) 
{
    StatusCode ret = StatusCode::sOk;

    LOG_INFO << "create_transaction begin";

    /*check snapshot status*/
    ret = m_snapshot_proxy->create_transaction(shead, snap_name);
    if(ret != StatusCode::sOk){
        LOG_ERROR << "snapshot proxy create transaction failed ret:" << ret;
        return ret;
    }

    /*check snapshot correspondent backup creating
     *if backup not ok, delete the backup and snapshot
     */
    string backup_name = snap_to_backup_name(snap_name);
    while(check_sync_on(backup_name)){
        usleep(200);
    }

    BackupStatus backup_status;
    /*check bakcup status*/
    ret = m_backup_inner_rpc_client->GetBackup(m_vol_name, backup_name, backup_status);
    if(ret != StatusCode::sOk || backup_status != BackupStatus::BACKUP_CREATING){
        /*delete snapshot*/ 
        ret = m_snapshot_proxy->do_update(shead, snap_name, UpdateEvent::DELETE_EVENT);
        /*delete backup*/
        ret = m_backup_inner_rpc_client->DeleteBackup(m_vol_name, backup_name);
        LOG_INFO << "create transaction recycle fail backup";
    }
    
    LOG_INFO << "create_transaction end";
    return ret;
}

StatusCode BackupDecorator::delete_transaction(const SnapReqHead& shead, const string& snap_name) 
{
    return m_snapshot_proxy->delete_transaction(shead, snap_name);
}

StatusCode BackupDecorator::rollback_transaction(const SnapReqHead& shead, const string& snap_name) 
{
    return m_snapshot_proxy->delete_transaction(shead, snap_name);
}

void BackupDecorator::add_sync(const string& actor, const string& action)
{
    m_sync_table.insert({actor, action});
}

void BackupDecorator::del_sync(const string& actor)
{
    m_sync_table.erase(actor);
}

bool BackupDecorator::check_sync_on(const string& actor)
{
    auto it = m_sync_table.find(actor);
    if(it == m_sync_table.end())
        return false;
    return true;
}