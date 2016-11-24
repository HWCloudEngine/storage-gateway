#ifndef CONTROL_CLIENT_HPP_
#define CONTROL_CLIENT_HPP_
#include <string>
#include <memory>
#include <grpc++/grpc++.h>
#include "../control.grpc.pb.h"

using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using huawei::proto::CtrlRpcSvc;

using huawei::proto::CreateSnapshotReq;
using huawei::proto::CreateSnapshotAck;
using huawei::proto::ListSnapshotReq;
using huawei::proto::ListSnapshotAck;
using huawei::proto::DeleteSnapshotReq;
using huawei::proto::DeleteSnapshotAck;
using huawei::proto::RollbackSnapshotReq;
using huawei::proto::RollbackSnapshotAck;
using huawei::proto::DiffSnapshotReq;
using huawei::proto::DiffSnapshotAck;
using huawei::proto::ReadSnapshotReq;
using huawei::proto::ReadSnapshotAck;

/*snapshot and other control rpc client*/
class ControlClient {
public:
    ControlClient(shared_ptr<Channel> channel) 
        :m_ctrl_stub(CtrlRpcSvc::NewStub(channel)){
    } 

    ~ControlClient(){

    }

    int CreateSnapshot(const std::string& vol_name, const string& snap_name){
        CreateSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        CreateSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->CreateSnapshot(&context, req, &ack);
        return ack.ret();
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
        return ack.ret();
    }
    
    int DeleteSnapshot(const string& vol_name, const string& snap_name){
        DeleteSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        DeleteSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->DeleteSnapshot(&context, req, &ack);
        return ack.ret();
    }

    int RollbackSnapshot(const string& vol_name, const string& snap_name){
        RollbackSnapshotReq req;
        req.set_vol_name(vol_name);
        req.set_snap_name(snap_name);
        RollbackSnapshotAck ack;
        ClientContext context;
        Status status = m_ctrl_stub->RollbackSnapshot(&context, req, &ack);
        return ack.ret();
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
        return ack.ret();
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
        return ack.ret();
    }

private:
    unique_ptr<CtrlRpcSvc::Stub> m_ctrl_stub;
};

#endif 
