#ifndef SNAPSHOT_MGR_H_
#define SNAPSHOT_MGR_H_

#include <string>
#include <map>
#include <memory>
#include <mutex>

#include <grpc++/grpc++.h>
#include "../rpc/common.pb.h"
#include "../rpc/snapshot_inner_control.grpc.pb.h"
#include "snapshot_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using huawei::proto::StatusCode;
using huawei::proto::inner::SnapshotInnerControl;
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

/*work on storage gateway server, all snapshot api gateway */
class SnapshotMgr final: public SnapshotInnerControl::Service {

public:
    SnapshotMgr(){
        m_all_snapmds.clear();
    }

    virtual ~SnapshotMgr(){
        m_all_snapmds.clear();
    }

public:
    /*call by sgserver when add and delete volume*/
    StatusCode add_volume(const string& vol_name, const size_t& vol_size);
    StatusCode del_volume(const string& vol_name);
    
    /*rpc interface*/
    grpc::Status Sync(ServerContext* context, const SyncReq* req, SyncAck* ack) override;
    grpc::Status Create(ServerContext* context, const CreateReq* req, CreateAck* ack) override;
    grpc::Status List(ServerContext* context, const ListReq* req, ListAck* ack) override;
    grpc::Status Query(ServerContext* context, const QueryReq* req, QueryAck* ack) override;
    grpc::Status Delete(ServerContext* context, const DeleteReq* req, DeleteAck* ack) override;
    grpc::Status Rollback(ServerContext* context, const RollbackReq* req, RollbackAck* ack) override;
    grpc::Status Update(ServerContext* context, const UpdateReq* req, UpdateAck* ack) override;
    grpc::Status CowOp(ServerContext* context, const CowReq* req, CowAck* ack) override;
    grpc::Status CowUpdate(ServerContext* context, const CowUpdateReq* req, CowUpdateAck* ack) override;
    grpc::Status Diff(ServerContext* context, const DiffReq* req, DiffAck* ack) override;
    grpc::Status Read(ServerContext* context, const ReadReq* req, ReadAck* ack) override;

private:
    /*each volume has a snapshot mds*/
    mutex m_mutex;
    map<string, shared_ptr<SnapshotMds>> m_all_snapmds;
};

#endif
