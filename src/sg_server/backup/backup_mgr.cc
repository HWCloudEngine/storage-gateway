/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
* File name:    backup_mgr.h
* Author:
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup rpc dispatch
*
***********************************************/
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "log/log.h"
#include "backup_mgr.h"

using huawei::proto::StatusCode;

#define CMD_PREV(vol, op)            \
do {                                 \
    string log_msg = "BackupMgr";    \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    LOG_INFO << log_msg;             \
}while(0)

#define CMD_POST(vol, op, ret)       \
do {                                 \
    string log_msg = "BackupMgr";    \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    if (!ret) {                      \
        log_msg += " ok";               \
        LOG_INFO << log_msg;            \
        return grpc::Status::OK;        \
    } else {                            \
        log_msg += " failed";           \
        LOG_INFO << log_msg;            \
        return grpc::Status::CANCELLED; \
    }                                   \
}while(0)

#define CMD_POST_1(vol, op, ret)     \
do {                                 \
    string log_msg = "BackupMgr";    \
    log_msg += " ";                  \
    log_msg += op;                   \
    log_msg += " ";                  \
    log_msg += "vname:";             \
    log_msg += vol;                  \
    if (!ret) {                      \
        log_msg += " ok";               \
        LOG_INFO << log_msg;            \
        return ret;                     \
    } else {                            \
        log_msg += " failed";           \
        LOG_INFO << log_msg;            \
        return ret;                     \
    }                                   \
}while(0)

#define CMD_DO(vname, op, req, ack)        \
do {                                       \
    auto it = m_all_backupmds.find(vname); \
    if (it == m_all_backupmds.end()) {     \
        ret = StatusCode::sVolumeNotExist; \
        break;                             \
    }                                      \
    ret = it->second->op(req, ack);        \
}while(0);

StatusCode BackupMgr::add_volume(const string& vol_name,
                                 const size_t& vol_size) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_all_backupmds.find(vol_name);
    if (it != m_all_backupmds.end()) {
        LOG_INFO << "add volume:" << vol_name << "failed, already exist";
        return StatusCode::sVolumeAlreadyExist;
    }

    shared_ptr<BackupMds> backup_mds;
    backup_mds.reset(new BackupMds(vol_name, vol_size));
    m_all_backupmds.insert({vol_name, backup_mds});
    backup_mds->recover();

    return StatusCode::sOk;
}

StatusCode BackupMgr::del_volume(const string& vol_name) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_all_backupmds.erase(vol_name);
    return StatusCode::sOk;
}

grpc::Status BackupMgr::Create(ServerContext* context,
                               const CreateBackupInReq* req,
                               CreateBackupInAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();
    size_t vsize = req->vol_size();
    auto it = m_all_backupmds.find(vname);
    shared_ptr<BackupMds> backup_mds;
    if (it != m_all_backupmds.end()) {
        backup_mds = it->second;
        goto create;
    }

    /*(todo debug only)create snapshotmds for each volume*/
    backup_mds.reset(new BackupMds(vname, vsize));
    m_all_backupmds.insert({vname, backup_mds});

create:
    CMD_PREV(vname, "create");
    ret = backup_mds->create_backup(req, ack);
    CMD_POST(vname, "create", ret);
}

grpc::Status BackupMgr::List(ServerContext* context,
                             const ListBackupInReq* req,
                             ListBackupInAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "List");
    CMD_DO(vname, list_backup, req, ack);
    CMD_POST(vname, "List", ret);
}

grpc::Status BackupMgr::Get(ServerContext* context,
                            const GetBackupInReq* req,
                            GetBackupInAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Get");
    CMD_DO(vname, get_backup, req, ack);
    CMD_POST(vname, "Get", ret);
}

grpc::Status BackupMgr::Delete(ServerContext* context,
                               const DeleteBackupInReq* req,
                               DeleteBackupInAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Delete");
    CMD_DO(vname, delete_backup, req, ack);
    CMD_POST(vname, "Delete", ret);
}

grpc::Status BackupMgr::Restore(ServerContext* context,
                                const RestoreBackupInReq* req,
                                ServerWriter<RestoreBackupInAck>* writer) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Restore");
    CMD_DO(vname, restore_backup, req, writer);
    CMD_POST(vname, "Restore", ret);
}


StatusCode BackupMgr::handle_remote_create_start(const RemoteBackupStartReq* req,
                                                 RemoteBackupStartAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();
    size_t vsize = req->vol_size();
    auto it = m_all_backupmds.find(vname);
    shared_ptr<BackupMds> backup_mds;
    if (it != m_all_backupmds.end()) {
        backup_mds = it->second;
        goto start;
    }
    backup_mds.reset(new BackupMds(vname, vsize));
    m_all_backupmds.insert({vname, backup_mds});

start:
    CMD_PREV(vname, "create remote start");
    ret = backup_mds->do_remote_create_start(req, ack);
    CMD_POST_1(vname, "create remote start", ret);
}

StatusCode BackupMgr::handle_remote_create_end(const RemoteBackupEndReq* req,
                                               RemoteBackupEndAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "create remote end");
    CMD_DO(vname, do_remote_create_end, req, ack);
    CMD_POST_1(vname, "create remote end", ret);
}

StatusCode BackupMgr::handle_remote_create_upload(UploadDataReq* req,
                                                  UploadDataAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "create remote upload");
    CMD_DO(vname, do_remote_create_upload, req, ack);
    CMD_POST_1(vname, "create remote upload", ret);
}

StatusCode BackupMgr::handle_remote_delete(const RemoteBackupDeleteReq* req,
                                           RemoteBackupDeleteAck* ack) {
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "delete remote");
    CMD_DO(vname, do_remote_delete, req, ack);
    CMD_POST_1(vname, "delete remote", ret);
}

StatusCode BackupMgr::handle_download(const DownloadDataReq* req,
            ServerReaderWriter<TransferResponse, TransferRequest>* stream) {
    StatusCode ret;
    string vname = req->vol_name();
    CMD_PREV(vname, "download");
    CMD_DO(vname, do_remote_download, req, stream);
    CMD_POST_1(vname, "download", ret);
}
