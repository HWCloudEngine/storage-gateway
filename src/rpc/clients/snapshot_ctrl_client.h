#ifndef SNAPSHOT_CONTROL_CLIENT_H_
#define SNAPSHOT_CONTROL_CLIENT_H_
#include <string>
#include <memory>
#include <grpc++/grpc++.h>
#include "../snapshot_control.grpc.pb.h"

using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using huawei::proto::SnapStatus;
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
class SnapCtrlClient {
public:
    SnapCtrlClient(shared_ptr<Channel> channel) 
        :m_ctrl_stub(SnapshotControl::NewStub(channel)){
    } 

    ~SnapCtrlClient(){

    }

    int CreateSnapshot(const std::string& vol_name, const string& snap_name){
        CreateSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        CreateSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->CreateSnapshot(&context, req, &ack);
        return (int)ack.header().status();
    }

    int ListSnapshot(const string& vol_name, set<string>& snap_set){
        ListSnapshotReq req;
        req.set_vol_name(vol_name);
        ListSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->ListSnapshot(&context, req, &ack);
        cout << "ListSnapshot size:" << ack.snap_name_size() << endl;
        for(int i = 0; i < ack.snap_name_size(); i++){
            cout << "ListSnapshot snapshot:" << ack.snap_name(i) << endl;
            snap_set.insert(ack.snap_name(i));
        }
        return (int)ack.header().status();
    }
    
    int QuerySnapshot(const string& vol_name, const string& snap_name, 
                      SnapStatus& snap_status) {
        QuerySnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        QuerySnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->QuerySnapshot(&context, req, &ack);
        snap_status = ack.snap_status();
        return (int)ack.header().status();
    }

    int DeleteSnapshot(const string& vol_name, const string& snap_name){
        DeleteSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        DeleteSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->DeleteSnapshot(&context, req, &ack);
        return (int)ack.header().status();
    }

    int RollbackSnapshot(const string& vol_name, const string& snap_name){
        RollbackSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        RollbackSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->RollbackSnapshot(&context, req, &ack);
        return (int)ack.header().status();
    }

    int DiffSnapshot(const string& vol_name, 
                     const string& first_snap_name, 
                     const string& last_snap_name){
        DiffSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_first_snap_name(first_snap_name);
        req.set_last_snap_name(last_snap_name);
        DiffSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->DiffSnapshot(&context, req, &ack);
        return (int)ack.header().status();
    }

    int ReadSnapshot(const string& vol_name, 
                     const string& snap_name,
                     const char*  buf,
                     const size_t len,
                     const off_t  off){
        ReadSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        req.set_off(off);
        req.set_len(len);
        ReadSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->ReadSnapshot(&context, req, &ack);
        return (int)ack.header().status();
    }

private:
    unique_ptr<SnapshotControl::Stub> m_ctrl_stub;
};

#endif 
