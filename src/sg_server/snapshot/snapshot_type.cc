/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot_type.cc
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot macro definition
* 
*************************************************/
#include <sstream>
#include <string.h>
#include "snapshot_type.h"

std::string snap_t::key() {
    return snap_prefix + std::to_string(snap_id);
}

std::string snap_t::encode() {
    std::string buf;
    buf.append("vol:");
    buf.append(vol_id);
    buf.append(" ");
    buf.append("rep:");
    buf.append(rep_id);
    buf.append(" ");
    buf.append("ckp:");
    buf.append(ckp_id);
    buf.append(" ");
    buf.append("snap_name:");
    buf.append(snap_name);
    buf.append(" ");
    buf.append("snap_id:");
    buf.append(std::to_string(snap_id));
    buf.append(" ");
    buf.append("snap_type:");
    buf.append(std::to_string(snap_type));
    buf.append(" ");
    buf.append("snap_status:");
    buf.append(std::to_string(snap_status));
    return buf;
}

void snap_t::decode(const std::string& buf) {
    std::istringstream ss(buf);
    std::string token; 
    std::string key;
    std::string val;
    while (ss >> token) {
        size_t pos = token.find(":");
        if (pos == -1) {
            continue;
        }
        key = token.substr(0, pos);
        val = token.substr(pos+1, token.length());
        if (key.compare("vol") == 0) {
            vol_id = val;
            continue;
        }
        if (key.compare("rep") == 0) {
            rep_id = val;
            continue;
        }
        if (key.compare("ckp") == 0) {
            ckp_id = val;
            continue;
        }
        if (key.compare("snap_name") == 0) {
            snap_name = val;
            continue;
        }
        if (key.compare("snap_id") == 0) {
            snap_id = std::atol(val.c_str());
            continue;
        }
        if (key.compare("snap_type") == 0) {
            snap_type = (snap_type_t)std::atoi(val.c_str());
            continue;
        }
        if (key.compare("snap_status") == 0) {
            snap_status = (snap_status_t)std::atol(val.c_str());
            continue;
        }
    }
}

std::string cow_block_t::prefix() {
    return block_prefix + std::to_string(snap_id) + "@";
}

std::string cow_block_t::key() {
    return block_prefix + std::to_string(snap_id) + "@" + std::to_string(block_id);
}

std::string cow_block_t::encode() {
    std::string buf;
    buf.append("snap_id:");
    buf.append(std::to_string(snap_id));
    buf.append(" ");
    buf.append("block_id:");
    buf.append(std::to_string(block_id));
    buf.append(" ");
    buf.append("block_zero:");
    buf.append(std::to_string(block_zero));
    buf.append(" ");
    buf.append("block_url:");
    buf.append(block_url);
    return buf;
}

void cow_block_t::decode(const std::string& buf) {
    std::istringstream ss(buf);
    std::string token; 
    std::string key;
    std::string val;
    while (ss >> token) {
        size_t pos = token.find(":");
        if (pos == -1) {
            continue;
        }
        key = token.substr(0, pos);
        val = token.substr(pos+1, token.length());
        if (key.compare("snap_id") == 0) {
            snap_id = std::atoi(val.c_str());
            continue;
        }
        if (key.compare("block_id") == 0) {
            block_id = std::atoi(val.c_str());
            continue;
        }
        if (key.compare("block_zero") == 0) {
            block_zero = std::atoi(val.c_str());
            continue;
        }
        if (key.compare("block_url") == 0) {
            block_url = val;
            continue;
        }
    }
}

std::string cow_block_ref_t::key() {
    return block_ref_prefix  + block_url;
}

std::string cow_block_ref_t::encode() {
    std::string buf;
    buf.append("block_url:");
    buf.append(block_url);
    buf.append(" ");
    buf.append("block_ref:");
    for (auto snap : block_url_refs) {
        buf.append(std::to_string(snap));
        buf.append("@");
    }
    return buf;
}

void cow_block_ref_t::decode(const std::string& buf) {
    std::istringstream ss(buf);
    std::string token; 
    std::string key;
    std::string val;
    while (ss >> token) {
       size_t  pos = token.find(":");
        if (pos == -1) {
            continue;
        }
        key = token.substr(0, pos);
        val = token.substr(pos+1, token.length());
        if (key.compare("block_url") == 0) {
            block_url = val;
            continue;
        }
        if (key.compare("block_ref") == 0) {
            char* source_str = const_cast<char*>(val.c_str());
            const char* split_str = "@";
            char* p = strtok(source_str, split_str);
            while (p != NULL) {
                snap_id_t id = std::atol(p);
                block_url_refs.insert(id);
                p = strtok(NULL, split_str);
            }
        }
    }
}
