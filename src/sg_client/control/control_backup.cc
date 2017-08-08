/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    control_backup.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup control interface export to highlevel control layer 
*
*************************************************/
#include "common/utils.h"
#include "rpc/backup.pb.h"
#include "control_backup.h"

using huawei::proto::StatusCode;
using huawei::proto::BackupOption;

shared_ptr<BackupProxy> BackupControlImpl::get_vol_backup_proxy(const string& vol_name) {
    auto it = m_volumes.find(vol_name);
    if (it != m_volumes.end()) {
        return it->second->get_backup_proxy();
    }
    LOG_ERROR << "get_vol_backup_proxy vid:" << vol_name << " failed";
    return nullptr;
}

Status BackupControlImpl::CreateBackup(ServerContext* context,
                                       const CreateBackupReq* req,
                                       CreateBackupAck* ack) {
    string vname = req->vol_name();
    uint64_t vsize = req->vol_size();

    LOG_INFO << "rpc CreateBackup vname:" << vname << " vsize:" << vsize;
    shared_ptr<BackupProxy> vol_backup_proxy = get_vol_backup_proxy(vname);
    assert(vol_backup_proxy != nullptr);
    /*create backup*/
    StatusCode ret = vol_backup_proxy->create_backup(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc CreateBackup vname:" << vname  << " failed" << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc CreateBackup vname:" << vname << " vsize:" << vsize << " ok";
    return Status::OK;
}

Status BackupControlImpl::GetBackup(ServerContext* context,
                                    const GetBackupReq* req,
                                    GetBackupAck* ack) {
    string vname = req->vol_name();
    LOG_INFO << "rpc QueryBackup vname:" << vname;
    shared_ptr<BackupProxy> vol_backup_proxy = get_vol_backup_proxy(vname);
    assert(vol_backup_proxy != nullptr);
    string backup_name = req->backup_name();
    StatusCode ret = vol_backup_proxy->get_backup(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc QueryBackup vname:" << vname << " failed" << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc QueryBackup vname:" << vname << " ok";
    return Status::OK;
}

Status BackupControlImpl::DeleteBackup(ServerContext* context,
                                       const DeleteBackupReq* req,
                                       DeleteBackupAck* ack) {
    string vname = req->vol_name();
    LOG_INFO << "rpc DeleteBackup" << " vname:" << vname;
    shared_ptr<BackupProxy> vol_backup_proxy = get_vol_backup_proxy(vname);
    assert(vol_backup_proxy != nullptr);
    StatusCode ret = vol_backup_proxy->delete_backup(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc DeleteBackup vname:" << vname << " failed" << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc DeleteBackup" << " vname:" << vname << " ok";
    return Status::OK;
}

Status BackupControlImpl::RestoreBackup(ServerContext* context,
                                        const RestoreBackupReq* req,
                                        RestoreBackupAck* ack) {
    string vname = req->vol_name();
    size_t vsize = req->vol_size();
    LOG_INFO << "rpc RestoreBackup" << " vname:" << vname;
    VolumeAttr vol_attr(vname, vsize);
    shared_ptr<BackupProxy> vol_backup_proxy = get_vol_backup_proxy(vname);
    if (vol_backup_proxy == nullptr) {
        vol_backup_proxy.reset(new BackupProxy(vol_attr, nullptr));
    }
    assert(vol_backup_proxy != nullptr);
    StatusCode ret = vol_backup_proxy->restore_backup(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc RestoreBackup vname:" << vname << " failed" << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc RestoreBackup" << " vname:" << vname << " ok";
    return Status::OK;
}

Status BackupControlImpl::ListBackup(ServerContext* context,
                                     const ListBackupReq* req,
                                     ListBackupAck* ack) {
    string vname = req->vol_name();
    LOG_INFO << "rpc ListBackup" << " vname:" << vname;
    shared_ptr<BackupProxy> vol_backup_proxy = get_vol_backup_proxy(vname);
    assert(vol_backup_proxy != nullptr);
    /*dispatch to volume*/
    StatusCode ret = vol_backup_proxy->list_backup(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc ListBackup vname:" << vname << " failed" << " err:" << ret;
        return Status::OK;
    }

    LOG_INFO << "rpc ListBackup" << " vname:" << vname << " ok";
    return Status::OK;
}
