#ifndef CONTROL_BACKUP_H_
#define CONTROL_BACKUP_H_

#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <functional>
#include <grpc++/grpc++.h>
#include "../../rpc/common.pb.h"
#include "../../rpc/backup_control.pb.h"
#include "../../rpc/backup_control.grpc.pb.h"
#include "../../log/log.h"
#include "../../backup/backup_proxy.h"
#include "../volume.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using huawei::proto::control::BackupControl;

using huawei::proto::control::CreateBackupReq;
using huawei::proto::control::CreateBackupAck;
using huawei::proto::control::DeleteBackupReq;
using huawei::proto::control::DeleteBackupAck;
using huawei::proto::control::RestoreBackupReq;
using huawei::proto::control::RestoreBackupAck;
using huawei::proto::control::QueryBackupReq;
using huawei::proto::control::QueryBackupAck;
using huawei::proto::control::ListBackupReq;
using huawei::proto::control::ListBackupAck;

using namespace std;
using namespace Journal;

class BackupControlImpl final: public BackupControl::Service 
{ 
public:
    BackupControlImpl(map<string, shared_ptr<Volume>>& volumes)
        :m_volumes(volumes){
    }

    virtual ~BackupControlImpl(){
    }

    Status CreateBackup(ServerContext* context, 
                        const CreateBackupReq* req, 
                        CreateBackupAck* ack) override;
    
    Status DeleteBackup(ServerContext* context, 
                        const DeleteBackupReq* req, 
                        DeleteBackupAck* ack) override;

    Status RestoreBackup(ServerContext* context, 
                         const RestoreBackupReq* req, 
                         RestoreBackupAck* ack) override;

    Status QueryBackup(ServerContext* contex,
                       const QueryBackupReq* req,
                       QueryBackupAck* ack) override;

    Status ListBackup(ServerContext* contex,
                      const ListBackupReq* req,
                      ListBackupAck* ack) override;
private:
    /*each volume own corresponding snapshot proxy*/
    shared_ptr<BackupProxy>   get_vol_backup_proxy(const string& vol_name);

private:
    map<string, shared_ptr<Volume>>& m_volumes;
};

#endif
