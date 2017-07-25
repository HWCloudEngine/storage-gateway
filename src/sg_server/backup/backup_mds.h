/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_mds.h
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  maintain backup metadata
* 
***********************************************/
#ifndef SRC_SG_SERVER_BACKUP_BACKUP_MDS_H_
#define SRC_SG_SERVER_BACKUP_BACKUP_MDS_H_
#include <string>
#include <memory>
#include <mutex>
#include <list>
#include <atomic>
#include <grpc++/grpc++.h>
#include "rpc/common.pb.h"
#include "rpc/backup.pb.h"
#include "rpc/transfer.pb.h"
#include "rpc/backup_inner_control.pb.h"
#include "rpc/backup_inner_control.grpc.pb.h"
#include "common/thread_pool.h"
#include "backup_ctx.h"
#include "backup_task.h"

using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerReaderWriter;
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

using sg_threads::ThreadPool;

class BackupMds {
 public:
    BackupMds(const std::string& vol_name, const size_t& vol_size);
    virtual ~BackupMds();

    /*backup rpc operation*/
    StatusCode create_backup(const CreateBackupInReq* req, CreateBackupInAck* ack);
    StatusCode delete_backup(const DeleteBackupInReq* req, DeleteBackupInAck* ack);
    StatusCode restore_backup(const RestoreBackupInReq* req, ServerWriter<RestoreBackupInAck>* writer);
    StatusCode list_backup(const ListBackupInReq* req, ListBackupInAck* ack);
    StatusCode get_backup(const GetBackupInReq* req, GetBackupInAck* ack);
    /*hanlde remote backup*/
    StatusCode do_remote_create_start(const RemoteBackupStartReq* req, RemoteBackupStartAck* ack);
    StatusCode do_remote_create_end(const RemoteBackupEndReq* req, RemoteBackupEndAck* ack);
    StatusCode do_remote_create_upload(UploadDataReq* req, UploadDataAck* ack);
    StatusCode do_remote_delete(const RemoteBackupDeleteReq* req, RemoteBackupDeleteAck* ack);
    StatusCode do_remote_download(const DownloadDataReq* req, ServerReaderWriter<TransferResponse, TransferRequest>* stream);
    /*crash recover*/
    int recover();
    /*track all backup task*/
    int do_task_schedule();

 private:
    StatusCode prepare_create(const std::string& bname,
                              const BackupMode& bmode,
                              const BackupType& btype, bool on_remote);
    StatusCode prepare_delete(const std::string& bname);

    StatusCode local_restore(const std::string& bname,
                             ServerWriter<RestoreBackupInAck>* writer);
    StatusCode remote_restore(const std::string& bname,
                              ServerWriter<RestoreBackupInAck>* writer);

 private:
    shared_ptr<BackupCtx> m_ctx;
    /*schedule all kind of task*/
    mutex m_task_schedule_lock;
    list<shared_ptr<AsyncTask>> m_task_schedule_list;
    atomic_bool m_task_schedule_run;
    /*all task will run in thread pool*/
    shared_ptr<ThreadPool> m_thread_pool;
};

#endif  // SRC_SG_SERVER_BACKUP_BACKUP_MDS_H_
