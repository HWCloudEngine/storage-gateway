#ifndef BACKUP_MGR_H_
#define BACKUP_MGR_H_
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "backup_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using huawei::proto::StatusCode;
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
using huawei::proto::inner::CreateRemoteBackupInReq;
using huawei::proto::inner::CreateRemoteBackupInAck;
using huawei::proto::inner::DeleteRemoteBackupInReq;
using huawei::proto::inner::DeleteRemoteBackupInAck;
using huawei::proto::inner::UploadReq;
using huawei::proto::inner::UploadAck;
using huawei::proto::inner::DownloadReq;
using huawei::proto::inner::DownloadAck;


using namespace std;

/*work on storage gateway server, all snapshot api gateway */
class BackupMgr final: public BackupInnerControl::Service 
{

public:
    BackupMgr(){}
    virtual ~BackupMgr(){}

public:
    /*call by sgserver when add and delete volume*/
    StatusCode add_volume(const string& vol_name, const size_t& vol_size);
    StatusCode del_volume(const string& vol_name);
    
    /*rpc interface*/
    grpc::Status Create(ServerContext* context, const CreateBackupInReq* req, 
                        CreateBackupInAck* ack);
    grpc::Status List(ServerContext* context, const ListBackupInReq* req, 
                      ListBackupInAck* ack);
    grpc::Status Get(ServerContext* context, const GetBackupInReq* req, 
                     GetBackupInAck* ack);
    grpc::Status Delete(ServerContext* context, const DeleteBackupInReq* req, 
                        DeleteBackupInAck* ack);
    grpc::Status Restore(ServerContext* context, const RestoreBackupInReq* req, 
                         ServerWriter<RestoreBackupInAck>* writer);
    
    /*todo mulitiplex with replication*/
    /*create and delete backup on remote site*/
    grpc::Status CreateRemote(ServerContext* context, const CreateRemoteBackupInReq* req, 
                              CreateRemoteBackupInAck* ack);
    grpc::Status DeleteRemote(ServerContext* context, const DeleteRemoteBackupInReq* req, 
                              DeleteRemoteBackupInAck* ack);
	/*upload backup data from stream of local sgserver*/
    grpc::Status Upload(ServerContext* context, ServerReader<UploadReq>* reader, 
                        UploadAck* ack);
	/*download backup data to stream of local sgserver*/
    grpc::Status Download(ServerContext* context, const DownloadReq* req, 
                          ServerWriter<DownloadAck>* writer);
  
private:
    /*each volume has a snapshot mds*/
    mutex m_mutex;
    map<string, shared_ptr<BackupMds>> m_all_backupmds;
};

#endif
