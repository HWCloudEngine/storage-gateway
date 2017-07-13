/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    backup_proxy.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup entry
*  
*************************************************/
#ifndef SRC_SG_CLIENT_BACKUP_BACKUP_DECORATOR_H_
#define SRC_SG_CLIENT_BACKUP_BACKUP_DECORATOR_H_
#include "../snapshot/snapshot_proxy.h"
#include "rpc/clients/rpc_client.h"

/*backup decorator decorate snapshot proxy about backup type snapshot*/
class BackupDecorator : public ISnapshot, public ITransaction, public ISyncBarrier {
 public:
    BackupDecorator(std::string vol_name, shared_ptr<SnapshotProxy> snapshot_proxy);
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
    StatusCode create_transaction(const SnapReqHead& shead, const std::string& snap_name) override;
    StatusCode delete_transaction(const SnapReqHead& shead, const std::string& snap_name) override;
    StatusCode rollback_transaction(const SnapReqHead& shead, const std::string& snap_name) override;
    /*syncbarrier*/
    void add_sync(const std::string& actor, const std::string& action) override;
    void del_sync(const std::string& actor) override;
    bool check_sync_on(const std::string& actor) override;

 private:
    std::string m_vol_name;
    shared_ptr<SnapshotProxy> m_snapshot_proxy;
    map<std::string, std::string> m_sync_table;
};

#endif  // SRC_SG_CLIENT_BACKUP_BACKUP_DECORATOR_H_
