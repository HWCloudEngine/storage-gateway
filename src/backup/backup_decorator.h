#ifndef BACKUP_DECORATOR_H
#define BACKUP_DECORATOR_H
#include "../snapshot/snapshot_proxy.h"

/*backup decorator decorate snapshot proxy about backup type snapshot*/
class BackupDecorator : public ISnapshot, public ITransaction, public ISyncBarrier
{
public:
    BackupDecorator(string vol_name, shared_ptr<SnapshotProxy> snapshot_proxy);
    ~BackupDecorator();

    /*snapshot*/
    StatusCode create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack) override;
    StatusCode delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack) override;
    StatusCode rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack) override;
    StatusCode list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack)override;
    StatusCode query_snapshot(const QuerySnapshotReq* req, QuerySnapshotAck* ack) override;
    StatusCode diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack) override;
    StatusCode read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack) override;

    /*transaction*/
    StatusCode create_transaction(const SnapReqHead& shead, const string& snap_name) override;
    StatusCode delete_transaction(const SnapReqHead& shead, const string& snap_name) override;
    StatusCode rollback_transaction(const SnapReqHead& shead, const string& snap_name) override;

    /*syncbarrier*/
    void add_sync(const string& actor, const string& action) override;
    void del_sync(const string& actor) override;
    bool check_sync_on(const string& actor) override;

private:
    string m_vol_name;
    shared_ptr<SnapshotProxy>         m_snapshot_proxy;
    shared_ptr<BackupInnerCtrlClient> m_backup_inner_rpc_client;
    map<string, string>               m_sync_table;
};

#endif
