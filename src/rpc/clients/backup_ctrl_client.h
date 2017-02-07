#ifndef SNAPSHOT_CONTROL_CLIENT_H_
#define SNAPSHOT_CONTROL_CLIENT_H_
#include <string>
#include <memory>
#include <set>
#include <vector>
#include <grpc++/grpc++.h>
#include "../backup.pb.h"
#include "../backup_control.pb.h"
#include "../backup_control.grpc.pb.h"

using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using huawei::proto::control::BackupControl;
using huawei::proto::control::CreateBackupReq;
using huawei::proto::control::CreateBackupAck;
using huawei::proto::control::ListBackupReq;
using huawei::proto::control::ListBackupAck;
using huawei::proto::control::QueryBackupReq;
using huawei::proto::control::QueryBackupAck;
using huawei::proto::control::DeleteBackupReq;
using huawei::proto::control::DeleteBackupAck;
using huawei::proto::control::RestoreBackupReq;
using huawei::proto::control::RestoreBackupAck;

/*backup control rpc client*/
class BackupCtrlClient 
{
public:
    BackupCtrlClient(shared_ptr<Channel> channel) 
        :m_stub(BackupControl::NewStub(channel)){
    } 

    ~BackupCtrlClient(){
    }

    StatusCode CreateBackup(const string& vol_name, 
                            const size_t& vol_size,
                            const string& backup_name,
                            const BackupOption& backup_option){
        CreateBackupReq req;
        req.set_vol_name(vol_name);
        req.set_vol_size(vol_size);
        req.set_backup_name(backup_name);
        req.mutable_backup_option()->CopyFrom(backup_option);

        CreateBackupAck ack;
        ClientContext context;
        grpc::Status status = m_stub->CreateBackup(&context, req, &ack);
        return ack.header().status();
    }

    StatusCode ListBackup(const string& vol_name, set<string>& backup_set){
        ListBackupReq req;
        req.set_vol_name(vol_name);
        ListBackupAck ack;
        ClientContext context;
        grpc::Status status = m_stub->ListBackup(&context, req, &ack);
        cout << "ListBackup size:" << ack.backup_name_size() << endl;
        for(int i = 0; i < ack.backup_name_size(); i++){
            cout << "ListBackup backup:" << ack.backup_name(i) << endl;
            backup_set.insert(ack.backup_name(i));
        }
        return ack.header().status();
    }
    
    StatusCode QueryBackup(const string& vol_name, const string& backup_name, 
                           BackupStatus& backup_status) {
        QueryBackupReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        QueryBackupAck ack;
        ClientContext context;
        grpc::Status status = m_stub->QueryBackup(&context, req, &ack);
        backup_status = ack.backup_status();
        return ack.header().status();
    }

    StatusCode DeleteBackup(const string& vol_name, const string& backup_name){
        DeleteBackupReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        DeleteBackupAck ack;
        ClientContext context;
        grpc::Status status = m_stub->DeleteBackup(&context, req, &ack);
        return ack.header().status();
    }

    StatusCode RestorebackBackup(const string& vol_name, 
                                 const string& backup_name,
                                 const string& new_vol_name, 
                                 const size_t& new_vol_size,
                                 const string& new_block_device){
        RestoreBackupReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        req.set_new_vol_name(new_vol_name);
        req.set_new_vol_size(new_vol_size);
        req.set_new_block_device(new_block_device);
        RestoreBackupAck ack;
        ClientContext context;
        grpc::Status status = m_stub->RestoreBackup(&context, req, &ack);
        return ack.header().status();
    }

private:
    unique_ptr<BackupControl::Stub> m_stub;
};

#endif 
