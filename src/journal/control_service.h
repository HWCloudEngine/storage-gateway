#ifndef SNAPSHOT_SVC_H_
#define SNAPSHOT_SVC_H_
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "../rpc/control.grpc.pb.h"
#include "../log/log.h"
#include "../journal/volume_manager.hpp"
#include "../snapshot/snapshot_proxy.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::CtrlRpcSvc;

using huawei::proto::CreateSnapshotReq;
using huawei::proto::CreateSnapshotAck;
using huawei::proto::ListSnapshotReq;
using huawei::proto::ListSnapshotAck;
using huawei::proto::RollbackSnapshotReq;
using huawei::proto::RollbackSnapshotAck;
using huawei::proto::DeleteSnapshotReq;
using huawei::proto::DeleteSnapshotAck;
using huawei::proto::DiffSnapshotReq;
using huawei::proto::DiffSnapshotAck;
using huawei::proto::ReadSnapshotReq;
using huawei::proto::ReadSnapshotAck;

using namespace std;
using namespace Journal;

/*snapshot and other service service as northern interface for all volume*/
class ControlSvc final: public CtrlRpcSvc::Service { 
public:
    static ControlSvc* s_instance;
    static mutex*          s_mutex;

    static ControlSvc* GetInstance(map<string, shared_ptr<Volume>>& volumes){
        if(nullptr == s_instance){
            lock_guard<std::mutex> lock(*s_mutex); 
            if(nullptr == s_instance){
                s_instance = new ControlSvc(volumes); 
            }
        } 
        LOG_INFO << "ControlSvc GetInstance";
        return s_instance;
    }

private:
    ControlSvc(map<string, shared_ptr<Volume>>& volumes)
        :m_volumes(volumes)
    {
        start();
    }

    virtual ~ControlSvc()
    {
        stop();
        m_work_thr->join();
    }

    void start();
    void run();
    void stop();

    Status CreateSnapshot(ServerContext*           context, 
                          const CreateSnapshotReq* req, 
                          CreateSnapshotAck*       ack) override;
    
    Status ListSnapshot(ServerContext*         context, 
                        const ListSnapshotReq* req, 
                        ListSnapshotAck*       ack) override;

    Status DeleteSnapshot(ServerContext*           context, 
                          const DeleteSnapshotReq* req, 
                          DeleteSnapshotAck*       ack) override;

    Status RollbackSnapshot(ServerContext*             context, 
                            const RollbackSnapshotReq* req, 
                            RollbackSnapshotAck*       ack) override;

    Status DiffSnapshot(ServerContext*         context, 
                        const DiffSnapshotReq* req, 
                        DiffSnapshotAck*       ack) override;

    Status ReadSnapshot(ServerContext*         context, 
                        const ReadSnapshotReq* req, 
                        ReadSnapshotAck*       ack) override;

private:
    /*each volume own corresponding snapshot proxy*/
    shared_ptr<SnapshotProxy> get_vol_snap_proxy(const string& vol_name);

private:
    string                    m_rpc_addr;
    unique_ptr<ServerBuilder> m_rpc_builder{nullptr};
    unique_ptr<Server>        m_rpc_server{nullptr};
    unique_ptr<thread>        m_work_thr{nullptr};

    map<string, shared_ptr<Volume>>& m_volumes;
};

#endif
