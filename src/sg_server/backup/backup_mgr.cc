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

grpc::Status BackupMgr::Create(ServerContext* context, 
                               const CreateBackupInReq* req, 
                               CreateBackupInAck* ack) 
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
                             ListBackupInAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "List");
    CMD_DO(vname, list_backup, req, ack);
    CMD_POST(vname, "List", ret);
}

grpc::Status BackupMgr::Get(ServerContext* context, 
                            const GetBackupInReq* req, 
                            GetBackupInAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Get");
    CMD_DO(vname, get_backup, req, ack);
    CMD_POST(vname, "Get", ret);
}

grpc::Status BackupMgr::Delete(ServerContext* context, 
                               const DeleteBackupInReq* req, 
                               DeleteBackupInAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Delete");
    CMD_DO(vname, delete_backup, req, ack);
    CMD_POST(vname, "Delete", ret);
}

grpc::Status BackupMgr::Restore(ServerContext* context, 
                                const RestoreBackupInReq* req, 
                                ServerWriter<RestoreBackupInAck>* writer)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "Restore");
    CMD_DO(vname, restore_backup, req, writer);
    CMD_POST(vname, "Restore", ret);
}

grpc::Status BackupMgr::CreateRemote(ServerContext* context, 
                                     const CreateRemoteBackupInReq* req, 
                                     CreateRemoteBackupInAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "create remote");
    CMD_DO(vname, create_remote, req, ack);
    CMD_POST(vname, "create remote", ret);
}

grpc::Status BackupMgr::DeleteRemote(ServerContext* context, 
                                     const DeleteRemoteBackupInReq* req, 
                                     DeleteRemoteBackupInAck* ack)
{
    StatusCode ret;
    string vname = req->vol_name();

    CMD_PREV(vname, "delete remote");
    CMD_DO(vname, delete_remote, req, ack);
    CMD_POST(vname, "delete remote", ret);
}

grpc::Status BackupMgr::Upload(ServerContext* context, 
                               ServerReader<UploadReq>* reader, 
                               UploadAck* ack)
{
    StatusCode ret;
    string vname;
    string bname;
    bool upload_start = false;
    UploadReq req; 

    while(reader->Read(&req))
    {
        vname = req.vol_name();
        auto it = m_all_backupmds.find(vname); 
        assert(it != m_all_backupmds.end()); 
        bname = req.backup_name();
        if(!upload_start){
            CMD_PREV(vname, "upload");
            it->second->upload_start(bname);
            upload_start = true;
        }
        uint64_t blk_no = req.blk_no();
        const string& blk_data = req.blk_data();
        it->second->upload(bname, blk_no, blk_data.c_str(), blk_data.length()); 
    }

    auto it = m_all_backupmds.find(vname); 
    assert(it != m_all_backupmds.end()); 
    it->second->upload_over(bname);

    CMD_POST(vname, "upload", ret);
}

grpc::Status BackupMgr::Download(ServerContext* context, const DownloadReq* req, 
                                 ServerWriter<DownloadAck>* writer)
{
    StatusCode ret;
    string vname = req->vol_name();
    CMD_PREV(vname, "upload");
    CMD_DO(vname, download, req, writer);
    CMD_POST(vname, "upload", ret);
}
