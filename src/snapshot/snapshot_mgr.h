#ifndef SNAPSHOT_MGR_H_
#define SNAPSHOT_MGR_H_

#include <string>
#include <map>
#include <memory>

#include <grpc++/grpc++.h>
#include "../rpc/snapshot.grpc.pb.h"
#include "snapshot_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::SnapshotRpcSvc;

using huawei::proto::CreateReq;
using huawei::proto::CreateAck;
using huawei::proto::ListReq;
using huawei::proto::ListAck;
using huawei::proto::DeleteReq;
using huawei::proto::DeleteAck;
using huawei::proto::RollbackReq;
using huawei::proto::RollbackAck;
using huawei::proto::UpdateReq;
using huawei::proto::UpdateAck;
using huawei::proto::CowReq;
using huawei::proto::CowAck;
using huawei::proto::CowUpdateReq;
using huawei::proto::CowUpdateAck;
using huawei::proto::DiffReq;
using huawei::proto::DiffAck;
using huawei::proto::ReadReq;
using huawei::proto::ReadAck;
using huawei::proto::SyncReq;
using huawei::proto::SyncAck;
using namespace std;

/*work on storage gateway server, all snapshot api gateway */
class SnapshotMgr final: public SnapshotRpcSvc::Service {

public:
    SnapshotMgr(){
        init();
    }

    virtual ~SnapshotMgr(){
        fini();
    }

public:
    void init();
    void fini();

    grpc::Status Sync(ServerContext* context, 
                      const SyncReq* req, 
                      SyncAck* ack) override;

    grpc::Status Create(ServerContext* context, 
                        const CreateReq* req, 
                        CreateAck* ack) override;

    grpc::Status List(ServerContext* context, 
                      const ListReq* req, 
                      ListAck* ack) override;

    grpc::Status Delete(ServerContext* context, 
                        const DeleteReq* req, 
                        DeleteAck* ack) override;

    grpc::Status Rollback(ServerContext* context, 
                          const RollbackReq* req, 
                          RollbackAck* ack) override;

    grpc::Status Update(ServerContext* context,
                        const UpdateReq* req,
                        UpdateAck* ack) override;
    
    grpc::Status CowOp(ServerContext* context,
                       const CowReq*  req,
                       CowAck* ack) override;

    grpc::Status CowUpdate(ServerContext* context,
                           const CowUpdateReq* req,
                           CowUpdateAck* ack) override;

    grpc::Status Diff(ServerContext* context, 
                      const DiffReq* req, 
                      DiffAck* ack) override;

    grpc::Status Read(ServerContext* context, 
                      const ReadReq* req, 
                      ReadAck* ack) override;

private:
    /*each volume has a snapshotmds*/
    map<string, shared_ptr<SnapshotMds>> m_volumes;
};

#endif
