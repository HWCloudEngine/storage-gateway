#ifndef CONTROL_SNAPSHOT_H_
#define CONTROL_SNAPSHOT_H_

#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "../../rpc/common.pb.h"
#include "../../rpc/snapshot_control.pb.h"
#include "../../rpc/snapshot_control.grpc.pb.h"
#include "../../log/log.h"
#include "../../snapshot/snapshot_proxy.h"
#include "../volume.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::control::SnapshotControl;
using huawei::proto::control::CreateSnapshotReq;
using huawei::proto::control::CreateSnapshotAck;
using huawei::proto::control::ListSnapshotReq;
using huawei::proto::control::ListSnapshotAck;
using huawei::proto::control::RollbackSnapshotReq;
using huawei::proto::control::RollbackSnapshotAck;
using huawei::proto::control::DeleteSnapshotReq;
using huawei::proto::control::DeleteSnapshotAck;
using huawei::proto::control::DiffSnapshotReq;
using huawei::proto::control::DiffSnapshotAck;
using huawei::proto::control::ReadSnapshotReq;
using huawei::proto::control::ReadSnapshotAck;

using namespace std;
using namespace Journal;

/*snapshot and other service service as northern interface for all volume*/
class SnapshotControlImpl final: public SnapshotControl::Service 
{ 
public:
    SnapshotControlImpl(map<string, shared_ptr<Volume>>& volumes)
        :m_volumes(volumes){
    }

    virtual ~SnapshotControlImpl(){
    }

    Status CreateSnapshot(ServerContext* context, 
                          const CreateSnapshotReq* req, 
                          CreateSnapshotAck* ack) override;
    
    Status ListSnapshot(ServerContext* context, 
                        const ListSnapshotReq* req, 
                        ListSnapshotAck* ack) override;
    
    Status QuerySnapshot(ServerContext* contex,
                         const QuerySnapshotReq* req,
                         QuerySnapshotAck* ack) override;

    Status DeleteSnapshot(ServerContext* context, 
                          const DeleteSnapshotReq* req, 
                          DeleteSnapshotAck* ack) override;

    Status RollbackSnapshot(ServerContext* context, 
                            const RollbackSnapshotReq* req, 
                            RollbackSnapshotAck* ack) override;

    Status DiffSnapshot(ServerContext* context, 
                        const DiffSnapshotReq* req, 
                        DiffSnapshotAck* ack) override;

    Status ReadSnapshot(ServerContext* context, 
                        const ReadSnapshotReq* req, 
                        ReadSnapshotAck* ack) override;

private:
    /*each volume own corresponding snapshot proxy*/
    shared_ptr<SnapshotProxy> get_vol_snap_proxy(const string& vol_name);

private:
    map<string, shared_ptr<Volume>>& m_volumes;
};

#endif
