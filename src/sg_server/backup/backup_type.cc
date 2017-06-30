/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_util.cc
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup type
* 
***********************************************/
#include <sstream>
#include <string.h>
#include "log/log.h"
#include "backup_type.h"

std::string backup_t::key() {
    return backup_prefix + std::to_string(backup_id);
}

std::string backup_t::encode() {
    std::string buf;
    buf.append("vol:");
    buf.append(vol_name);
    buf.append(" ");
    buf.append("bmode:");
    buf.append(std::to_string(backup_mode));
    buf.append(" ");
    buf.append("bname:");
    buf.append(backup_name);
    buf.append(" ");
    buf.append("bid:");
    buf.append(std::to_string(backup_id));
    buf.append(" ");
    buf.append("bstatus:");
    buf.append(std::to_string(backup_status));
    buf.append(" ");
    buf.append("btype:");
    buf.append(std::to_string(backup_type));
    return buf;
}

void backup_t::decode(const std::string& buf) {
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
            vol_name = val;
            continue;
        }
        if (key.compare("bmode") == 0) {
            backup_mode = (BackupMode)atoi(val.c_str());
            continue;
        }
        if (key.compare("bname") == 0) {
            backup_name = val;
            continue;
        }
        if (key.compare("bid") == 0) {
            backup_id = atoi(val.c_str());
            continue;
        }
        if (key.compare("bstatus") == 0) {
            backup_status = (BackupStatus)std::atoi(val.c_str());
            continue;
        }
        if (key.compare("btype") == 0) {
            backup_type = (BackupType)std::atoi(val.c_str());
            continue;
        }
    }
}

std::string backup_block_t::prefix() {
    return backup_block_prefix + std::to_string(backup_id) + "@";
}

std::string backup_block_t::key() {
    return backup_block_prefix + std::to_string(backup_id) + "@" + std::to_string(block_no);
}

std::string backup_block_t::encode() {
    std::string buf;
    buf.append("vol:");
    buf.append(vol_name);
    buf.append(" ");
    buf.append("backup_id:");
    buf.append(std::to_string(backup_id));
    buf.append(" ");
    buf.append("block_no:");
    buf.append(std::to_string(block_no));
    buf.append(" ");
    buf.append("block_zero:");
    buf.append(std::to_string(block_zero));
    buf.append(" ");
    buf.append("block_url:");
    buf.append(block_url);
    return buf;
}

void backup_block_t::decode(const std::string& buf) {
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
            vol_name = val;
            continue;
        }
        if (key.compare("backup_id") == 0) {
            backup_id = std::atoi(val.c_str());
            continue;
        }
        if (key.compare("block_no") == 0) {
            block_no = std::atol(val.c_str());
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

std::string spawn_backup_block_url(const backup_block_t& block) { 
    std::string block_url = block.vol_name;
    block_url.append("@");
    block_url.append(std::to_string(block.backup_id));
    block_url.append("@");
    block_url.append(std::to_string(block.block_no));
    block_url.append(".backup.obj");
    return block_url; 
}
