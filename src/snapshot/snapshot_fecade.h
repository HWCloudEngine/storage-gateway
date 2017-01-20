#ifndef SNAPSHOT_FECADE_H_
#define SNAPSHOT_FECADE_H_

#include <string>
#include <map>
#include <memory>
#include <grpc++/grpc++.h>
#include "../rpc/common.pb.h"
#include "../rpc/snapshot_inner_control.grpc.pb.h"
#include "snapshot_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::inner::CreateReq;
using huawei::proto::inner::CreateAck;
using huawei::proto::inner::ListReq;
using huawei::proto::inner::ListAck;
using huawei::proto::inner::QueryReq;
using huawei::proto::inner::QueryAck;
using huawei::proto::inner::DeleteReq;
using huawei::proto::inner::DeleteAck;
using huawei::proto::inner::RollbackReq;
using huawei::proto::inner::RollbackAck;
using huawei::proto::inner::UpdateReq;
using huawei::proto::inner::UpdateAck;
using huawei::proto::inner::CowReq;
using huawei::proto::inner::CowAck;
using huawei::proto::inner::CowUpdateReq;
using huawei::proto::inner::CowUpdateAck;
using huawei::proto::inner::DiffReq;
using huawei::proto::inner::DiffAck;
using huawei::proto::inner::ReadReq;
using huawei::proto::inner::ReadAck;
using huawei::proto::inner::SyncReq;
using huawei::proto::inner::SyncAck;

using namespace std;

/*each volume has a SnapshotFecade manage snapshot, replication and backup metadata*/
class SnapshotFecade
{

public:
    SnapshotFecade(const string vol_name);
    ~SnapshotFecade();
    
    int recover();

    grpc::Status Sync(const SyncReq* req, SyncAck* ack);
    grpc::Status Create(const CreateReq* req, CreateAck* ack);
    grpc::Status List(const ListReq* req, ListAck* ack);
    grpc::Status Query(const QueryReq* req, QueryAck* ack);
    grpc::Status Delete(const DeleteReq* req, DeleteAck* ack);
    grpc::Status Rollback(const RollbackReq* req, RollbackAck* ack);
    grpc::Status Update(const UpdateReq* req, UpdateAck* ack);
    grpc::Status CowOp(const CowReq*  req, CowAck* ack);
    grpc::Status CowUpdate(const CowUpdateReq* req, CowUpdateAck* ack);
    grpc::Status Diff(const DiffReq* req, DiffAck* ack);
    grpc::Status Read(const ReadReq* req, ReadAck* ack);

private:
    string m_vol_name;
    /*manage normal snapshot*/
    SnapshotMds*  m_snap_mds;
    
    /*manage replication (decorator of SnapshotMds)*/

    /*manage backup (decorator of SnapshotMds)*/
};

#endif
