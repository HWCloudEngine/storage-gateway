#ifndef SNAPSHOT_CTRL_CLIENT_H_
#define SNAPSHOT_CTRL_CLIENT_H_
#include <string>
#include <memory>
#include <set>
#include <vector>
#include <grpc++/grpc++.h>
#include "../log/log.h"
#include "../snapshot.pb.h"
#include "../snapshot_control.grpc.pb.h"

using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using huawei::proto::SnapStatus;
using huawei::proto::DiffBlocks;
using huawei::proto::StatusCode;

using huawei::proto::control::SnapshotControl;
using huawei::proto::control::CreateSnapshotReq;
using huawei::proto::control::CreateSnapshotAck;
using huawei::proto::control::ListSnapshotReq;
using huawei::proto::control::ListSnapshotAck;
using huawei::proto::control::QuerySnapshotReq;
using huawei::proto::control::QuerySnapshotAck;
using huawei::proto::control::DeleteSnapshotReq;
using huawei::proto::control::DeleteSnapshotAck;
using huawei::proto::control::RollbackSnapshotReq;
using huawei::proto::control::RollbackSnapshotAck;
using huawei::proto::control::DiffSnapshotReq;
using huawei::proto::control::DiffSnapshotAck;
using huawei::proto::control::ReadSnapshotReq;
using huawei::proto::control::ReadSnapshotAck;

/*snapshot and other control rpc client*/
class SnapshotCtrlClient 
{
public:
    SnapshotCtrlClient(shared_ptr<Channel> channel) 
        :m_ctrl_stub(SnapshotControl::NewStub(channel)){
    } 

    ~SnapshotCtrlClient(){
    }

    StatusCode CreateSnapshot(const string& vol_name, const string& snap_name){
        CreateSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        CreateSnapshotAck ack;
        ClientContext context;
        grpc::Status status = m_ctrl_stub->CreateSnapshot(&context, req, &ack);
        return ack.header().status();
    }

    StatusCode ListSnapshot(const string& vol_name, set<string>& snap_set){
        ListSnapshotReq req;
        req.set_vol_name(vol_name);
        ListSnapshotAck ack;
        ClientContext context;
        grpc::Status status = m_ctrl_stub->ListSnapshot(&context, req, &ack);
        for(int i = 0; i < ack.snap_name_size(); i++){
            snap_set.insert(ack.snap_name(i));
        }
        return ack.header().status();
    }
    
    StatusCode QuerySnapshot(const string& vol_name, const string& snap_name, 
                             SnapStatus& snap_status) {
        QuerySnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        QuerySnapshotAck ack;
        ClientContext context;
        grpc::Status status = m_ctrl_stub->QuerySnapshot(&context, req, &ack);
        snap_status = ack.snap_status();
        return ack.header().status();
    }

    StatusCode DeleteSnapshot(const string& vol_name, const string& snap_name){
        DeleteSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        DeleteSnapshotAck ack;
        ClientContext context;
        grpc::Status status = m_ctrl_stub->DeleteSnapshot(&context, req, &ack);
        return ack.header().status();
    }

    StatusCode RollbackSnapshot(const string& vol_name, const string& snap_name){
        RollbackSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        RollbackSnapshotAck ack;
        ClientContext context;
        grpc::Status status = m_ctrl_stub->RollbackSnapshot(&context, req, &ack);
        return ack.header().status();
    }

    StatusCode DiffSnapshot(const string& vol_name, 
                            const string& first_snap_name, 
                            const string& last_snap_name,
                            vector<DiffBlocks>& diff){
        DiffSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_first_snap_name(first_snap_name);
        req.set_last_snap_name(last_snap_name);
        DiffSnapshotAck ack;
        ClientContext context;
        grpc::Status status = m_ctrl_stub->DiffSnapshot(&context, req, &ack);
        
        /*todo: there may be too many diff blocks, should batch and split*/
        int count = ack.diff_blocks_size();
        for(int i = 0; i < count; i++){
            diff.push_back(ack.diff_blocks(i)); 
        }
        return ack.header().status();
    }

    StatusCode ReadSnapshot(const string& vol_name, const string& snap_name,
                            const char* buf, const size_t len, const off_t off){
        ReadSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        req.set_off(off);
        req.set_len(len);
        ReadSnapshotAck ack;
        ClientContext context;
        /*todo: current read snapshot request should send to 
         * machine which hold the block device, how to optimize*/
        grpc::Status status = m_ctrl_stub->ReadSnapshot(&context, req, &ack);
        if(!status.ok()){
            LOG_ERROR << "read snapshot len:" << len << "failed status:" << status.error_code();
            return ack.header().status();
        }
        memcpy((char*)buf, ack.data().data(), len);
        return ack.header().status();
    }

private:
    unique_ptr<SnapshotControl::Stub> m_ctrl_stub;
};

#endif 
