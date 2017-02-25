#ifndef ISNAPSHOT_H
#define ISNAPSHOT_H

#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/snapshot_control.pb.h"
#include "rpc/snapshot_control.grpc.pb.h"

using huawei::proto::StatusCode;
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
using huawei::proto::control::DiffSnapshotReq;
using huawei::proto::control::DiffSnapshotAck;
using huawei::proto::control::ReadSnapshotReq;
using huawei::proto::control::ReadSnapshotAck;


class ISnapshot
{
public:
    ISnapshot() = default;
    virtual ~ISnapshot(){}

    virtual StatusCode create_snapshot(const CreateSnapshotReq* req, CreateSnapshotAck* ack) = 0;
    virtual StatusCode delete_snapshot(const DeleteSnapshotReq* req, DeleteSnapshotAck* ack) = 0;
    virtual StatusCode rollback_snapshot(const RollbackSnapshotReq* req, RollbackSnapshotAck* ack) = 0;
    virtual StatusCode list_snapshot(const ListSnapshotReq* req, ListSnapshotAck* ack) = 0;
    virtual StatusCode query_snapshot(const QuerySnapshotReq* req, QuerySnapshotAck* ack) = 0;
    virtual StatusCode diff_snapshot(const DiffSnapshotReq* req, DiffSnapshotAck* ack) = 0;
    virtual StatusCode read_snapshot(const ReadSnapshotReq* req, ReadSnapshotAck* ack) = 0;
};

#endif
