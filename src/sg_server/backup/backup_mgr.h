#ifndef BACKUP_MGR_H_
#define BACKUP_MGR_H_
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/transfer.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "backup_mds.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerReaderWriter;
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

using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using huawei::proto::transfer::RemoteBackupStartReq;
using huawei::proto::transfer::RemoteBackupStartAck;
using huawei::proto::transfer::RemoteBackupEndReq;
using huawei::proto::transfer::RemoteBackupEndAck;
using huawei::proto::transfer::RemoteBackupDeleteReq;
using huawei::proto::transfer::RemoteBackupDeleteAck;
using huawei::proto::transfer::UploadDataReq;
using huawei::proto::transfer::UploadDataAck;
using huawei::proto::transfer::DownloadDataReq;
using huawei::proto::transfer::DownloadDataAck;


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


    /*register as message handler to net receiver*/
    StatusCode handle_remote_create_start(const RemoteBackupStartReq* req, 
                                          RemoteBackupStartAck* ack);

    StatusCode handle_remote_create_end(const RemoteBackupEndReq* req, 
                                         RemoteBackupEndAck* ack);

    StatusCode handle_remote_create_upload(UploadDataReq* req, UploadDataAck* ack);

    StatusCode handle_remote_delete(const RemoteBackupDeleteReq* req, 
                                    RemoteBackupDeleteAck* ack);
	
    StatusCode handle_download(const DownloadDataReq* req, 
                               ServerReaderWriter<TransferResponse,TransferRequest>* stream);
  
private:
    /*each volume has a snapshot mds*/
    mutex m_mutex;
    map<string, shared_ptr<BackupMds>> m_all_backupmds;
};

#endif
