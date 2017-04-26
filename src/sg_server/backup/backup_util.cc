/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_util.cc
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup utility
* 
***********************************************/
#include "log/log.h"
#include "backup_util.h"

std::string spawn_key(const std::string& prefix, const std::string& value) {
    std::string key = prefix;
    key.append(BACKUP_FS);
    key.append(value);
    return key;
}

void split_key(const std::string& raw_key, std::string& prefix,
               std::string& key) {
    size_t pos = raw_key.find(BACKUP_FS, 0);
    prefix = raw_key.substr(0, pos);
    key = raw_key.substr(pos+1, raw_key.length());
}

std::string spawn_latest_backup_id_key() {
    return spawn_key(BACKUP_ID_PREFIX, "");
}

std::string spawn_backup_attr_map_key(const std::string& backup_name) {
    return spawn_key(BACKUP_MAP_PREFIX, backup_name);
}

void split_backup_attr_map_key(const std::string& raw_key,
                               std::string& backup_name) {
    std::string prefix;
    split_key(raw_key, prefix, backup_name);
}

std::string spawn_backup_attr_map_val(const backup_attr_t& backup_attr) {
    std::string key;
    key += backup_attr.volume_uuid;
    key.append(BACKUP_FS);
    key += std::to_string(backup_attr.backup_mode);
    key.append(BACKUP_FS);
    key += backup_attr.backup_name;
    key.append(BACKUP_FS);
    key += std::to_string(backup_attr.backup_id);
    key.append(BACKUP_FS);
    key += std::to_string(backup_attr.backup_status);
    key.append(BACKUP_FS);
    key += std::to_string(backup_attr.backup_type);
    return key;
}

void split_backup_attr_map_val(const std::string& raw_key,
                               backup_attr_t& backup_attr) {
    LOG_INFO << "attr val:" << raw_key;
    std::string volume_uuid;
    std::string remain0;
    split_key(raw_key, volume_uuid, remain0);
    backup_attr.volume_uuid = volume_uuid;

    std::string backup_mode;
    std::string remain1;
    split_key(remain0, backup_mode, remain1);
    backup_attr.backup_mode = (BackupMode)atoi(backup_mode.c_str());

    std::string backup_name;
    std::string remain2;
    split_key(remain1, backup_name, remain2);
    backup_attr.backup_name = backup_name;

    std::string backup_id;
    std::string remain3;
    split_key(remain2, backup_id, remain3);
    backup_attr.backup_id = (backupid_t)atoi(backup_id.c_str());

    std::string backup_status;
    std::string remain4;
    split_key(remain3, backup_status, remain4);
    backup_attr.backup_status = (BackupStatus)atoi(backup_status.c_str());

    std::string backup_type;
    std::string remain5;
    split_key(remain4, backup_type, remain5);
    backup_attr.backup_type = (BackupType)atoi(backup_type.c_str());
}

std::string spawn_backup_block_map_key(const backupid_t& backup_id,
                                       const block_t& block_id) {
    std::string key = std::to_string(backup_id);
    key.append(BACKUP_FS);
    key += std::to_string(block_id);
    return spawn_key(BACKUP_BLOCK_PREFIX, key);
}

void split_backup_block_map_key(const std::string& raw_key,
                                backupid_t& backup_id,
                                block_t& block_id) {
    std::string prefix;
    std::string key;
    split_key(raw_key, prefix, key);

    std::string backup;
    std::string block;
    split_key(key, backup, block);

    backup_id = atol(backup.c_str());
    block_id  = atol(block.c_str());
}

std::string spawn_backup_object_name(const std::string& vol_name,
                                     const backupid_t& backup_id,
                                     const block_t& blk_id) {
    /*todo: may add pool name*/
    std::string backup_object_name = vol_name;
    backup_object_name.append(BACKUP_FS);
    backup_object_name.append(std::to_string(backup_id));
    backup_object_name.append(BACKUP_FS);
    backup_object_name.append(std::to_string(blk_id));
    backup_object_name.append(BACKUP_OBJ_SUFFIX);
    return backup_object_name;
}
