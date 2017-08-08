/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_ctx.h
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  maintain backup context
* 
***********************************************/
#ifndef SRC_SG_SERVER_BACKUP_BACKUP_CTX_H_
#define SRC_SG_SERVER_BACKUP_BACKUP_CTX_H_
#include <string>
#include <mutex>
#include <map>
#include "common/block_store.h"
#include "common/index_store.h"
#include "rpc/clients/snapshot_ctrl_client.h"
#include "backup_type.h"

class BackupCtx {
 public:
    BackupCtx() = default;
    explicit BackupCtx(const std::string& vol_name, const size_t& vol_size);
    BackupCtx(const BackupCtx& other) = delete;
    BackupCtx& operator=(const BackupCtx& other) = delete;
    ~BackupCtx();

    std::string vol_name()const;
    size_t vol_size()const;

    backupid_t set_latest_backup_id(const backupid_t& backup_id);
    backupid_t latest_backup_id()const;

    IndexStore* index_store()const;
    BlockStore* block_store()const;

    SnapshotCtrlClient* snap_client();

    bool is_backup_exist(const std::string& cur_backup);
    bool is_incr_backup_allowable();
    bool is_backup_deletable(const std::string& cur_backup);
    bool is_snapshot_valid(const std::string& cur_snap);

    backupid_t   get_backup_id(const std::string& cur_backup);
    std::string  get_backup_name(const backupid_t& cur_backup_id);
    BackupMode   get_backup_mode(const std::string& cur_backup);
    BackupStatus get_backup_status(const std::string& cur_backup);
    BackupType   get_backup_type(const std::string& cur_backup);

    bool update_backup_status(const string& cur_backup,
                              const BackupStatus& backup_status);

    /*the latest full backup in system*/
    std::string get_latest_full_backup();
    /*the latest backup in system*/
    std::string get_latest_backup();
    /*give a backup, get the base backup of the backup*/
    std::string get_backup_base(const std::string& cur_backup);

    /*according current backup name to get prev/next backup name*/
    std::string get_prev_backup(const std::string& cur_backup);
    std::string get_next_backup(const std::string& cur_backup);

    backupid_t spawn_backup_id();

    /*debug*/
    void trace();
    
 private:
    /*volume basic*/
    std::string m_vol_name;
    size_t m_vol_size;
    /*backup basic*/
    std::recursive_mutex m_mutex;
    backupid_t m_latest_backup_id;
    /*index store for backup meta*/
    IndexStore* m_index_store;
    /*block store for backup data*/
    BlockStore* m_block_store;

    /*snapshot client for reading incremental data and metadata */
    std::string m_snap_client_ip;
    SnapshotCtrlClient* m_snap_client;
};

#endif  // SRC_SG_SERVER_BACKUP_BACKUP_CTX_H_
