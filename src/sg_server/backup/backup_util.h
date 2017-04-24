/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_util.h
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup utility
* 
***********************************************/
#ifndef SRC_SG_SERVER_BACKUP_BACKUP_UTIL_H_
#define SRC_SG_SERVER_BACKUP_BACKUP_UTIL_H_
#include "backup_def.h"

/*helper function to handle db persist key*/
std::string spawn_key(const std::string& prefix, const std::string& value);
void split_key(const std::string& raw, std::string& prefix, std::string& key);

std::string spawn_latest_backup_id_key();

std::string spawn_backup_attr_map_key(const std::string& backup_name);
void split_backup_attr_map_key(const std::string& raw_key,
                               std::string& backup_name);
std::string spawn_backup_attr_map_val(const backup_attr_t& backup_attr);
void split_backup_attr_map_val(const std::string& raw_key,
                               backup_attr_t& backup_attr);

std::string spawn_backup_block_map_key(const backupid_t & backup_id,
                                       const block_t& block_id);
void split_backup_block_map_key(const std::string& raw_key,
                                backupid_t& backup_id, block_t& block_id);

std::string spawn_backup_object_name(const std::string& vol_name,
                                     const backupid_t& backup_id,
                                     const block_t& blk_id);

#endif  // SRC_SG_SERVER_BACKUP_BACKUP_UTIL_H_
