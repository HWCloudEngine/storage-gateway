/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_def.h
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup macro definition
* 
***********************************************/
#ifndef SRC_SG_SERVER_BACKUP_BACKUP_TYPE_H_
#define SRC_SG_SERVER_BACKUP_BACKUP_TYPE_H_
#include <string>
#include <set>
#include "rpc/common.pb.h"
#include "rpc/backup.pb.h"
using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;

/*block*/
typedef uint64_t block_t;
/*backup id*/
typedef uint64_t backupid_t;
/*backup data object name*/
typedef std::string backup_object_t;

/*backup attribution*/
struct backup_attr {
    std::string volume_uuid;
    BackupMode backup_mode;
    std::string backup_name;
    backupid_t backup_id;
    BackupStatus backup_status;
    BackupType backup_type;
};
typedef struct backup_attr backup_attr_t;

#define BACKUP_META "/backup"

#define BACKUP_FS "#"
#define BACKUP_OBJ_SUFFIX ".backupobj"

#define BACKUP_INIT_UUID (2222)

/*backup indexstore key prefix*/
#define BACKUP_ID_PREFIX      "backup_latestid"
#define BACKUP_MAP_PREFIX     "backup_map_prefix"
#define BACKUP_BLOCK_PREFIX   "backup_block_prefix"

#endif  // SRC_SG_SERVER_BACKUP_BACKUP_DEF_H_
