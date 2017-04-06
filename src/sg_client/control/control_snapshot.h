#ifndef CONTROL_SNAPSHOT_H_
#define CONTROL_SNAPSHOT_H_
#include <sys/time.h>
#include <map>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/snapshot_control.pb.h"
#include "rpc/snapshot_control.grpc.pb.h"
#include "../snapshot/snapshot_proxy.h"
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
using huawei::proto::control::CreateVolumeFromSnapReq;
using huawei::proto::control::CreateVolumeFromSnapAck;
using huawei::proto::control::QueryVolumeFromSnapReq;
using huawei::proto::control::QueryVolumeFromSnapAck;

using namespace std;
using namespace Journal;

/*snapshot and other service service as northern interface for all volume*/
class SnapshotControlImpl final: public SnapshotControl::Service 
{ 
public:
    SnapshotControlImpl(map<string, shared_ptr<Volume>>& volumes);
    virtual ~SnapshotControlImpl();

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

    Status CreateVolumeFromSnap(ServerContext* context, 
                                const CreateVolumeFromSnapReq* req, 
                                CreateVolumeFromSnapAck* ack) override;

    Status QueryVolumeFromSnap(ServerContext* context, 
                               const QueryVolumeFromSnapReq* req, 
                               QueryVolumeFromSnapAck* ack) override;

private:
    /*each volume own corresponding snapshot proxy*/
    shared_ptr<SnapshotProxy> get_vol_snap_proxy(const string& vol_name);
    
    /*create volume from snapshot*/
    bool is_bdev_available(const string& blk_device);
    bool is_snapshot_available(const string& vol, const string& snap);
    void bg_work();
    void bg_reclaim();

private:
    map<string, shared_ptr<Volume>>& m_volumes;

    /*create volume from snapshot*/
    const static int BG_INIT  = 0;
    const static int BG_DOING = 1;
    const static int BG_DONE  = 2;
    const static int BG_ERR   = 3;
    struct BgJob
    {
        BgJob(string new_vol, string new_blk, string vol, string snap)
        {
            new_volume = new_vol;
            new_blk_device = new_blk;
            vol_name = vol;
            snap_name = snap;
        }
        string new_volume;
        string new_blk_device;
        string vol_name;
        string snap_name;
        int    status;
        struct timeval start_ts;
        struct timeval complete_ts;
        struct timeval expire_ts;
    };
    /*todo background job should persit for recover*/
    BlockingQueue<struct BgJob*>* m_pending_queue;
    deque<struct BgJob*>* m_complete_queue;
    atomic_bool m_run;
    /*todo use thread pool */
    thread* m_work_thread;
    thread* m_reclaim_thread;
};

#endif
