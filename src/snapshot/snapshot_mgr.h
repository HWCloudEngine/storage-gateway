#ifndef SNAPSHOT_MGR_H_
#define SNAPSHOT_MGR_H_

#include <string>
#include <map>
#include <memory>

#include <grpc++/grpc++.h>
#include "../rpc/snapshot.grpc.pb.h"
#include "../log/log.h"
#include "snapshot_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::SnapshotRpcSvc;

using huawei::proto::GetUidReq;
using huawei::proto::GetUidAck;
using huawei::proto::CreateReq;
using huawei::proto::CreateAck;
using huawei::proto::ListReq;
using huawei::proto::ListAck;
using huawei::proto::DeleteReq;
using huawei::proto::DeleteAck;
using huawei::proto::SetStatusReq;
using huawei::proto::SetStatusAck;
using huawei::proto::GetStatusReq;
using huawei::proto::GetStatusAck;
using huawei::proto::CowReq;
using huawei::proto::CowAck;
using huawei::proto::CowUpdateReq;
using huawei::proto::CowUpdateAck;
using huawei::proto::RollbackReq;
using huawei::proto::RollbackAck;
using huawei::proto::DiffReq;
using huawei::proto::DiffAck;
using huawei::proto::ReadReq;
using huawei::proto::ReadAck;
using huawei::proto::GetSnapshotIdReq;
using huawei::proto::GetSnapshotIdAck;
using huawei::proto::GetSnapshotIdReq;
using huawei::proto::GetSnapshotIdReq;

using namespace std;

/*work on storage gateway server, all snapshot RPC entry */
class SnapshotMgr final: public SnapshotRpcSvc::Service {

public:
    SnapshotMgr(){
        LOG_INFO << "RPC SnapshotMgr create";
    }

    virtual ~SnapshotMgr(){
        LOG_INFO << "RPC SnapshotMgr delete";
    }

public:
    grpc::Status GetUid(ServerContext*   context,
                        const GetUidReq* req,
                        GetUidAck*       ack) override;

    grpc::Status GetSnapshotName(ServerContext*            context,
                                 const GetSnapshotNameReq* req,
                                 GetSnapshotNameAck*       ack) override;

    grpc::Status GetSnapshotId(ServerContext*          context,
                               const GetSnapshotIdReq* req,
                               GetSnapshotIdAck*       ack) override;

    grpc::Status Create(ServerContext*   context, 
                        const CreateReq* req, 
                        CreateAck*       ack) override;

    grpc::Status List(ServerContext* context, 
                      const ListReq* req, 
                      ListAck*       ack) override;

    grpc::Status Delete(ServerContext*   context, 
                        const DeleteReq* req, 
                        DeleteAck*       ack) override;

    grpc::Status Rollback(ServerContext*     context, 
                          const RollbackReq* req, 
                          RollbackAck*       ack) override;

    grpc::Status SetStatus(ServerContext*      context,
                           const SetStatusReq* req,
                           SetStatusAck*       ack) override;

    grpc::Status GetStatus(ServerContext*      context,
                           const GetStatusReq* req,
                           GetStatusAck*       ack) override;
    
    grpc::Status CowOp(ServerContext* context,
                       const CowReq*  req,
                       CowAck*        ack) override;

    grpc::Status CowUpdate(ServerContext*      context,
                           const CowUpdateReq* req,
                           CowUpdateAck*       ack) override;

    grpc::Status Diff(ServerContext* context, 
                      const DiffReq* req, 
                      DiffAck*       ack) override;

    grpc::Status Read(ServerContext* context, 
                      const ReadReq* req, 
                      ReadAck*      ack) override;

private:
    map<string, shared_ptr<SnapshotMds>> m_volumes;
};

#endif
