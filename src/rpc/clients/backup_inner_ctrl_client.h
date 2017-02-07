#ifndef BACKUP_INNER_CTRL_CLIENT_H_
#define BACKUP_INNER_CTRL__CLIENT_H_

#include <string>
#include <memory>
#include <set>
#include <vector>
#include <grpc++/grpc++.h>
#include "../backup.pb.h"
#include "../backup_inner_control.pb.h"
#include "../backup_inner_control.grpc.pb.h"

using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using huawei::proto::StatusCode;
using huawei::proto::BackupMode;
using huawei::proto::BackupStatus;
using huawei::proto::BackupOption;
using huawei::proto::RestoreBlocks;

using huawei::proto::inner::BackupInnerControl;
using huawei::proto::inner::CreateBackupInReq;
using huawei::proto::inner::CreateBackupInAck;
using huawei::proto::inner::ListBackupInReq;
using huawei::proto::inner::ListBackupInAck;
using huawei::proto::inner::GetBackupInReq;
using huawei::proto::inner::GetBackupInAck;
using huawei::proto::inner::DeleteBackupInReq;
using huawei::proto::inner::DeleteBackupInAck;
using huawei::proto::inner::RestoreBackupInReq;
using huawei::proto::inner::RestoreBackupInAck;

/*backup control rpc client*/
class BackupInnerCtrlClient 
{
public:
    BackupInnerCtrlClient(shared_ptr<Channel> channel) 
        :m_stub(BackupInnerControl::NewStub(channel)){
    } 

    ~BackupInnerCtrlClient(){
    }

    StatusCode CreateBackup(const string& vol_name, 
                            const size_t& vol_size,
                            const string& backup_name,
                            const BackupOption& backup_option){
        CreateBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_vol_size(vol_size);
        req.set_backup_name(backup_name);
        req.mutable_backup_option()->CopyFrom(backup_option);

        CreateBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Create(&context, req, &ack);
        return ack.status();
    }

    StatusCode ListBackup(const string& vol_name, set<string>& backup_set){
        ListBackupInReq req;
        req.set_vol_name(vol_name);
        ListBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->List(&context, req, &ack);
        cout << "ListBackup size:" << ack.backup_name_size() << endl;
        for(int i = 0; i < ack.backup_name_size(); i++){
            cout << "ListBackup backup:" << ack.backup_name(i) << endl;
            backup_set.insert(ack.backup_name(i));
        }
        return ack.status();
    }
    
    StatusCode GetBackup(const string& vol_name, const string& backup_name, 
                           BackupStatus& backup_status) {
        GetBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        GetBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Get(&context, req, &ack);
        backup_status = ack.backup_status();
        return ack.status();
    }

    StatusCode DeleteBackup(const string& vol_name, const string& backup_name){
        DeleteBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        DeleteBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Delete(&context, req, &ack);
        return ack.status();
    }

    StatusCode RestoreBackup(const string& vol_name, 
                             const string& backup_name,
                             const string& new_vol_name, 
                             const size_t& new_vol_size,
                             const string& new_block_device,
                             vector<RestoreBlocks>& restore_blocks_vec){
        RestoreBackupInReq req;
        req.set_vol_name(vol_name);
        req.set_backup_name(backup_name);
        req.set_new_vol_name(new_vol_name);
        req.set_new_vol_size(new_vol_size);
        req.set_new_block_device(new_block_device);
        RestoreBackupInAck ack;
        ClientContext context;
        grpc::Status status = m_stub->Restore(&context, req, &ack);
        if(status.ok()){
            int restore_blocks_size = ack.restore_blocks_size();
            for(int i = 0; i < restore_blocks_size; i++){
                restore_blocks_vec.push_back(ack.restore_blocks(i));
            }
        }
        return ack.status();
    }

private:
    unique_ptr<BackupInnerControl::Stub> m_stub;
};

#endif 
