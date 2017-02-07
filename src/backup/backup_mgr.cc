#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "../log/log.h"
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
    if(!ret) {                       \
        log_msg += " ok";               \
        LOG_INFO << log_msg;            \
        return grpc::Status::OK;        \
    } else {                            \
        log_msg += " failed";           \
        LOG_INFO << log_msg;            \
        return grpc::Status::CANCELLED; \
    }                                   \
}while(0)

#define CMD_DO(vname, op, req, ack)        \
do {                                       \
    auto it = m_all_backupmds.find(vname); \
    if(it == m_all_backupmds.end()){       \
        ret = StatusCode::sVolumeNotExist; \
        break;                             \
    }                                      \
    ret = it->second->op(req, ack);        \
}while(0);

StatusCode BackupMgr::add_volume(const string& vol_name, const size_t& vol_size)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_all_backupmds.find(vol_name);
    if(it != m_all_backupmds.end()){
        LOG_INFO << "add volume:" << vol_name << "failed, already exist";
        return StatusCode::sVolumeAlreadyExist; 
    }

    shared_ptr<BackupMds> backup_mds;
    backup_mds.reset(new BackupMds(vol_name, vol_size));
    m_all_backupmds.insert({vol_name, backup_mds});
    backup_mds->recover();

    return StatusCode::sOk;
}

StatusCode BackupMgr::del_volume(const string& vol_name)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_all_backupmds.erase(vol_name);
    return StatusCode::sOk;
}

grpc::Status BackupMgr::Create(ServerContext*   context, 
                               const CreateBackupInReq* req, 
                               CreateBackupInAck*       ack) 
{
    StatusCode ret;
    string vname = req->vol_name();
    size_t vsize = req->vol_size();
    auto it = m_all_backupmds.find(vname);
    shared_ptr<BackupMds> backup_mds;
    if(it != m_all_backupmds.end()){
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
                             ListBackupInAck*       ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "List");
    CMD_DO(vname, list_backup, req, ack);
    CMD_POST(vname, "List", ret);
}

grpc::Status BackupMgr::Query(ServerContext* context, 
                              const QueryBackupInReq* req, 
                              QueryBackupInAck*       ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Query");
    CMD_DO(vname, query_backup, req, ack);
    CMD_POST(vname, "Query", ret);
}

grpc::Status BackupMgr::Delete(ServerContext*   context, 
                               const DeleteBackupInReq* req, 
                               DeleteBackupInAck*       ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Delete");
    CMD_DO(vname, delete_backup, req, ack);
    CMD_POST(vname, "Delete", ret);
}

grpc::Status BackupMgr::Restore(ServerContext*     context, 
                                const RestoreBackupInReq* req, 
                                RestoreBackupInAck*       ack) 
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Restore");
    CMD_DO(vname, restore_backup, req, ack);
    CMD_POST(vname, "Restore", ret);
}
