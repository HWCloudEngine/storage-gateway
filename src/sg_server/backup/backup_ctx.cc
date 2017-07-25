/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_mds.cc
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  maintain backup metadata
* 
***********************************************/
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "common/define.h"
#include "common/utils.h"
#include "common/config_option.h"
#include "log/log.h"
#include "rpc/snapshot.pb.h"
#include "rpc/volume.pb.h"
#include "../volume_inner_control.h"
#include "backup_ctx.h"
using huawei::proto::VolumeInfo;

BackupCtx::BackupCtx(const std::string& vol_name, const size_t& vol_size) {
    m_vol_name = vol_name;
    m_vol_size = vol_size;
    m_latest_backup_id = BACKUP_INIT_UUID;
    m_block_store = BlockStore::factory("local");
    if (!m_block_store) {
        LOG_ERROR << "backup block store create failed"; 
    }
    std::string db_path = g_option.local_meta_path + "/" + vol_name + "/backup";
    if (access(db_path.c_str(), F_OK)) {
        char cmd[256] = "";
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", db_path.c_str());
        int ret = system(cmd);
        assert(ret != -1);
    }
    m_index_store = IndexStore::create("rocksdb", db_path);
    if (!m_index_store) {
        LOG_ERROR << "backup index store create failed"; 
    }
    int ret = m_index_store->db_open();
    assert(ret == 0);
    m_snap_client_ip = g_option.ctrl_server_ip;
    m_snap_client_port = g_option.ctrl_server_port;
    if (network_reachable(m_snap_client_ip.c_str(), m_snap_client_port)) {
        std::string rpc_addr = rpc_address(m_snap_client_ip, g_option.ctrl_server_port);
        m_snap_client = new SnapshotCtrlClient(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials()));
        assert(m_snap_client != nullptr);
    } else {
        LOG_ERROR << "backup snap client network unreachable ip:" << m_snap_client_ip << " port:" << m_snap_client_port; 
    }
    g_vol_ctrl->add_observee(reinterpret_cast<Observee*>(this));
}

BackupCtx::~BackupCtx() {
    g_vol_ctrl->del_observee(reinterpret_cast<Observee*>(this));
    if (m_snap_client) {
        delete m_snap_client;
    }
    if (m_index_store) {
        delete m_index_store;
    }
    if (m_block_store) {
        delete m_block_store;
    }
}

std::string BackupCtx::vol_name()const {
    return m_vol_name;
}

size_t BackupCtx::vol_size()const {
    return m_vol_size;
}

backupid_t BackupCtx::set_latest_backup_id(const backupid_t& backup_id) {
    m_latest_backup_id = backup_id;
}

backupid_t BackupCtx::latest_backup_id()const {
    return m_latest_backup_id;
}

IndexStore* BackupCtx::index_store() const {
    return m_index_store;
}

BlockStore* BackupCtx::block_store()const {
    return m_block_store;
}

SnapshotCtrlClient* BackupCtx::snap_client()const {
    return m_snap_client;
}

bool BackupCtx::is_incr_backup_allowable() {
    /*if current exist no full backup, incr backup will be rejected*/
    std::string latest_full_backup = get_latest_full_backup();
    if (latest_full_backup.empty()) {
        LOG_ERROR << "failed: incr backup should have base full backup";
        return false;
    }
    std::string latest_backup = get_latest_backup();
    if (latest_backup.empty()) {
        LOG_ERROR << "failed: incr backup hasn't prev backup";
        return false;
    }
    BackupStatus latest_backup_status = get_backup_status(latest_backup);
    if (latest_backup_status != BackupStatus::BACKUP_AVAILABLE) {
        LOG_ERROR << "failed: incr backup's prev backup not available";
        return false;
    }
    /*check prev backup snapshot valid or not, if invalid, reject create backup*/
    std::string latest_backup_snap = backup_to_snap_name(latest_backup);
    if (latest_backup_snap.empty()) {
        LOG_ERROR << "failed: incr backup's prev backup snapshot not exist";
        return false;
    }
    return is_snapshot_valid(latest_backup_snap);
}

bool BackupCtx::is_backup_exist(const std::string& backup_name) {
    backupid_t id = get_backup_id(backup_name);
    if (-1 == id) {
        return false;
    }
    return true;
}

bool BackupCtx::is_backup_deletable(const string& backup_name) {
    backupid_t backup_id = get_backup_id(backup_name);
    if (-1 == backup_id) {
        LOG_ERROR << "backup:" << backup_name << " no exist";
        return false;
    }
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    backup_t current;
    current.vol_name = m_vol_name;
    current.backup_id = backup_id;
    it->seek_to_first(current.key());
    if (!it->valid()) {
        LOG_ERROR << "backup:" << backup_name << " no exist";
        return false;
    }
    current.decode(it->value());
    /*the current backup be restoring, can't delete */
    if (current.backup_status == BackupStatus::BACKUP_RESTORING) {
        LOG_ERROR << "backup:" << backup_name << " restoring cannot delete";
        return false;
    }
    /*any incr backup can be deleted*/
    if (current.backup_mode == BackupMode::BACKUP_INCR) {
        return true;
    }
    if (current.backup_mode == BackupMode::BACKUP_FULL) {
        std::string next_backup_name = get_next_backup(backup_name);
        if (!next_backup_name.empty()) {
            backup_t successor;
            successor.vol_name = m_vol_name;
            successor.backup_id = get_backup_id(next_backup_name);
            it->seek_to_first(successor.key());
            if (!it->valid()) {
                LOG_ERROR << "next backup:" << next_backup_name << " no exist";
                return false;
            }
            successor.decode(it->value());
            if (successor.backup_mode == BackupMode::BACKUP_INCR) {
                /*current full bakcup has incr backup base on it*/
                return false;
            }
        }
    }
    return true;
}

bool BackupCtx::is_snapshot_valid(const std::string& cur_snap) {
    SnapStatus cur_snap_status;
    m_snap_client->QuerySnapshot(m_vol_name, cur_snap, cur_snap_status);
    if (cur_snap_status != SnapStatus::SNAP_CREATED) {
        LOG_ERROR << "failed: snapshot:" << cur_snap << " not available";
        return false;
    }
    return true;
}

backupid_t BackupCtx::get_backup_id(const std::string& cur_backup) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (0 == current.backup_name.compare(cur_backup)) {
            return current.backup_id; 
        }
    }
    return -1;
}

std::string BackupCtx::get_backup_name(const backupid_t& backup_id) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (current.backup_id == backup_id) {
            return current.backup_name; 
        }
    }
    return "";
}

BackupMode BackupCtx::get_backup_mode(const std::string& cur_backup) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (0 == current.backup_name.compare(cur_backup)) {
            return current.backup_mode; 
        }
    }
}

BackupStatus BackupCtx::get_backup_status(const std::string& cur_backup) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (0 == current.backup_name.compare(cur_backup)) {
            return current.backup_status; 
        }
    }
}

BackupType BackupCtx::get_backup_type(const std::string& cur_backup) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (0 == current.backup_name.compare(cur_backup)) {
            return current.backup_type; 
        }
    }
}

bool BackupCtx::update_backup_status(const std::string& cur_backup,
                                     const BackupStatus& backup_status) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (0 == current.backup_name.compare(cur_backup)) {
            current.backup_status = backup_status;
            m_index_store->db_put(current.key(), current.encode());
            break;
        }
    }
    return true;
}

std::string BackupCtx::get_prev_backup(const std::string& cur_backup) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    backupid_t cur_backup_id = get_backup_id(cur_backup);
    backup_t current;
    current.vol_name = m_vol_name;
    current.backup_id = cur_backup_id;
    it->seek_to_first(current.key());
    if (!it->valid()) {
        LOG_ERROR << "current:" << cur_backup << " has no prev backup";
        return "";
    }
    it->prev();
    if (!it->valid()) {
        LOG_ERROR << "current:" << cur_backup << " has no prev backup";
        return "";
    }
    backup_t previous;
    previous.decode(it->value());
    return previous.backup_name;
}

std::string BackupCtx::get_next_backup(const std::string& cur_backup) {
    backupid_t cur_backup_id = get_backup_id(cur_backup);
    backup_t current;
    current.vol_name = m_vol_name;
    current.backup_id = cur_backup_id;
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(current.key());
    if (!it->valid()) {
        LOG_ERROR << "current:" << cur_backup << " has no next backup";
        return "";
    }
    it->next();
    if (!it->valid()) {
        LOG_ERROR << "current:" << cur_backup << " has no next backup";
        return "";
    }
    backup_t successor;
    successor.decode(it->value());
    return successor.backup_name;
}

std::string BackupCtx::get_latest_full_backup() {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_last(backup_prefix);
    if (!it->valid()) {
        LOG_ERROR << "no latest full backup";
        return "";
    }
    for (; it->valid()&&(-1 != it->key().find(backup_prefix)); it->prev()) {
        backup_t current;
        current.decode(it->value());
        if (current.backup_mode == BackupMode::BACKUP_FULL) {
            return current.backup_name; 
        }
    }
    return "";
}

std::string BackupCtx::get_latest_backup() {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_last(backup_prefix);
    if (!it->valid()) {
        LOG_ERROR << "no latest backup";
        return "";
    }
    backup_t current;
    current.decode(it->value());
    return current.backup_name;
}

std::string BackupCtx::get_backup_base(const std::string& cur_backup) {
    BackupMode cur_backup_mode = get_backup_mode(cur_backup);
    if (cur_backup_mode == BackupMode::BACKUP_FULL) {
        return cur_backup;
    } else {
        std::string prev_backup = get_prev_backup(cur_backup);
        while (!prev_backup.empty()) {
            BackupMode prev_backup_mode = get_backup_mode(prev_backup);
            if (prev_backup_mode == BackupMode::BACKUP_FULL) {
                return prev_backup;
            }
            prev_backup = get_prev_backup(prev_backup);
        }
    }
    return "";
}

backupid_t BackupCtx::spawn_backup_id() {
    lock_guard<std::recursive_mutex> lock(m_mutex);
    backupid_t backup_id = m_latest_backup_id;
    m_latest_backup_id++;
    return backup_id;
}

void BackupCtx::update(int event, void* arg) {
    if (event != UPDATE_VOLUME) {
        LOG_ERROR << "update event:" << event << " only updae volume support";
        return; 
    }
    VolumeInfo* vol = reinterpret_cast<VolumeInfo*>(arg);
    if (vol == nullptr) {
        LOG_ERROR << "update null ptr";
        return;
    }
    if (vol->attached_host().compare(m_snap_client_ip) == 0) {
        LOG_ERROR << "update ip no change";
        return;
    }
    if (m_snap_client) {
        delete m_snap_client;
    }
    m_snap_client_ip = vol->attached_host();
    if (!network_reachable(m_snap_client_ip.c_str(), m_snap_client_port)) {
        LOG_ERROR << "update ip:" << m_snap_client_ip << " port:" << m_snap_client_port << " unreachable";
        return;
    }
    std::string rpc_addr = rpc_address(m_snap_client_ip, m_snap_client_port);
    m_snap_client = new SnapshotCtrlClient(grpc::CreateChannel(rpc_addr, grpc::InsecureChannelCredentials()));
    assert(m_snap_client != nullptr);
}

void BackupCtx::trace() {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(backup_prefix);
    LOG_INFO << "\t current backup info";
    for (; it->valid() && (-1 != it->key().find(backup_prefix)); it->next()) {
        LOG_INFO << "\t\t" << it->key() << " " << it->value();
    }
    LOG_INFO << "\t current backup block info";
    IndexStore::IndexIterator it0 = m_index_store->db_iterator();
    it0->seek_to_first(backup_block_prefix);
    for (; it0->valid() && (-1 != it0->key().find(backup_block_prefix)); it0->next()) {
        LOG_INFO << "\t\t" << it0->key() << " " << it0->value();
    }
}
