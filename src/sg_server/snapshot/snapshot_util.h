/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_util.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot utility
* 
*************************************************/
#ifndef SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_UTIL_H_
#define SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_UTIL_H_
#include <string>
#include "common/define.h"
#include "snapshot_def.h"

/*helper function to handle db persist key*/
class DbUtil {
 public:
    /*common snapshot db persist relevant*/
    static std::string spawn_key(const std::string& prefix,
                                 const std::string& value);
    static void   split_key(const std::string& raw, std::string& prefix,
                            std::string& key);
    static std::string spawn_latest_id_key();
    static std::string spawn_latest_name_key();
    static std::string spawn_attr_map_key(const std::string& snap_name);
    static void split_attr_map_key(const std::string& raw_key,
                                   std::string& snap_name);
    static std::string spawn_attr_map_val(const snap_attr_t& snap_attr);
    static void split_attr_map_val(const std::string& raw_key,
                                   snap_attr_t& snap_attr);
    static std::string spawn_cow_block_map_key(const snapid_t& snap_id,
                                               const block_t& block_id);
    static void split_cow_block_map_key(const std::string& raw_key,
                                        snapid_t& snap_id,
                                        block_t& block_id);
    static std::string spawn_cow_object_map_key(const std::string& obj_name);
    static void split_cow_object_map_key(const std::string& raw_key,
                                         std::string& obj_name);
    static std::string spawn_cow_object_map_val(const cow_object_ref_t& obj_ref);
    static void split_cow_object_map_val(const std::string& raw_val,
                                         cow_object_ref_t& obj_ref);
};

#endif  // SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_UTIL_H_
