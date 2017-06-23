/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_type.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot macro definition
* 
*************************************************/
#ifndef SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_TYPE_H_
#define SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_TYPE_H_
#include <string>
#include <set>
#include "rpc/snapshot.pb.h"

using snap_id_t = uint64_t;
using snap_status_t = huawei::proto::SnapStatus;
using snap_type_t = huawei::proto::SnapType;

static const  uint32_t max_snap_id = 2000;

/*snapshot attribution*/
static const std::string snap_prefix = "METAA_SNAP@"; 
struct snap_t {
    std::string vol_id;
    /*replication id*/
    std::string rep_id;
    /*replication checkpoint id*/
    std::string ckp_id;
    std::string   snap_name;
    snap_id_t     snap_id;
    snap_type_t   snap_type;
    snap_status_t snap_status;
    /*persistent key string*/
    std::string key();
    std::string encode();
    void decode(const std::string& buf);
};

static const std::string block_prefix = "METAB_BLOCK@";
struct cow_block_t {
    snap_id_t   snap_id;
    uint64_t    block_id;
    bool        block_zero;
    std::string block_url;
    std::string prefix();
    std::string key();
    std::string encode();
    void decode(const std::string& buf);
};

static const std::string block_ref_prefix = "METAC_BREF@";
struct cow_block_ref_t {
    std::string         block_url;
    std::set<snap_id_t> block_url_refs;
    std::string key();
    std::string encode();
    void decode(const std::string& buf);
};

#endif  // SRC_SG_SERVER_SNAPSHOT_SNAPSHOT_TYPE_H_
