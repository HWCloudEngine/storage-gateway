/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_type.h
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup type
* 
***********************************************/
#ifndef SRC_SG_SERVER_BACKUP_BACKUP_TYPE_H_
#define SRC_SG_SERVER_BACKUP_BACKUP_TYPE_H_
#include <string>
#include "rpc/common.pb.h"
#include "rpc/backup.pb.h"
using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;

/*backup initial id*/
static const uint32_t BACKUP_INIT_UUID = 200;
typedef uint32_t backupid_t;
typedef uint64_t block_t;

static const std::string backup_prefix = "AMETA_BACKUPP@"; 
struct backup {
    std::string  vol_name;
    BackupMode   backup_mode;
    std::string  backup_name;
    backupid_t   backup_id;
    BackupStatus backup_status;
    BackupType   backup_type;

    std::string prefix();
    std::string key();
    std::string encode();
    void decode(const std::string& buf);
};
typedef struct backup backup_t;

static const std::string backup_block_prefix = "BMETA_BLOCK@"; 
struct backup_block {
    std::string  vol_name;
    backupid_t   backup_id;
    block_t      block_no;
    bool         block_zero;
    std::string  block_url;
    
    std::string prefix();
    std::string key();
    std::string encode();
    void decode(const std::string& buf);
};
typedef struct backup_block backup_block_t;

std::string spawn_backup_block_url(const backup_block_t& block);

#endif  // SRC_SG_SERVER_BACKUP_BACKUP_TYPE_H_
