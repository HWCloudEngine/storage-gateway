#ifndef _BACKUP_MDS_H
#define _BACKUP_MDS_H
#include <memory>
#include <mutex>
#include <list>
#include <atomic>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/backup.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "common/thread_pool.h"
#include "backup_ctx.h"
#include "backup_task.h"

using namespace std;
using grpc::ServerReader;
using grpc::ServerWriter;
using huawei::proto::StatusCode;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;
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

using sg_threads::ThreadPool;

class BackupMds 
{
public:
    BackupMds(const string& vol_name, const size_t& vol_size);
    virtual ~BackupMds();   

    /*backup rpc operation*/
    StatusCode create_backup(const CreateBackupInReq* req, CreateBackupInAck* ack);
    StatusCode delete_backup(const DeleteBackupInReq* req, DeleteBackupInAck* ack);
    StatusCode restore_backup(const RestoreBackupInReq* req,  ServerWriter<RestoreBackupInAck>* writer);
    StatusCode list_backup(const ListBackupInReq* req, ListBackupInAck* ack);
    StatusCode get_backup(const GetBackupInReq* req, GetBackupInAck* ack);

	/*call by remote backup*/
    StatusCode create_remote(const CreateRemoteBackupInReq* req, CreateRemoteBackupInAck* ack);
    StatusCode delete_remote(const DeleteRemoteBackupInReq* req, DeleteRemoteBackupInAck* ack);
    
    /*remote site receive backup data*/
    StatusCode upload_start(const string& backup_name);
	StatusCode upload(const string& backup_name, const block_t& blk_no, 
                      const char* blk_data, const size_t& blk_len);
    StatusCode upload_over(const string& backup_name);

    StatusCode download(const DownloadReq* req, ServerWriter<DownloadAck>* writer);
 
    /*crash recover*/
    int recover();

	/*track all backup task*/
	int trace_task();

private:
    StatusCode prepare_create(const string& bname, const BackupMode& bmode, 
                              const BackupType& btype);
    StatusCode prepare_delete(const string& bname);

private:
	shared_ptr<BackupCtx>        m_ctx;
    
    /*trace all kind of task*/
	mutex                        m_task_tracer_lock;
	list<shared_ptr<BackupTask>> m_task_tracer_list;
	atomic_bool                  m_task_tracer_run;
    
    /*all task will run in thread pool*/
    shared_ptr<ThreadPool> m_thread_pool;
};

#endif
