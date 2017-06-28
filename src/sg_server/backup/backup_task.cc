/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    backup_task.h
* Author: 
* Date:         2016/11/03
* Version:      1.0
* Description:  general backup async task
* 
***********************************************/
#include <vector>
#include <map>
#include <functional>
#include "log/log.h"
#include "common/define.h"
#include "common/utils.h"
#include "rpc/common.pb.h"
#include "rpc/transfer.pb.h"
#include "backup_util.h"
#include "backup_ctx.h"
#include "backup_task.h"

using huawei::proto::transfer::MessageType;
using huawei::proto::transfer::TransferRequest;
using huawei::proto::transfer::TransferResponse;
using huawei::proto::transfer::RemoteBackupStartReq;
using huawei::proto::transfer::RemoteBackupStartAck;
using huawei::proto::transfer::RemoteBackupEndReq;
using huawei::proto::transfer::RemoteBackupEndAck;
using huawei::proto::transfer::RemoteBackupDeleteReq;
using huawei::proto::transfer::RemoteBackupDeleteAck;
using huawei::proto::transfer::UploadDataReq;
using huawei::proto::transfer::UploadDataAck;
using huawei::proto::transfer::DownloadDataReq;
using huawei::proto::transfer::DownloadDataAck;


AsyncTask::AsyncTask(const string& backup_name, shared_ptr<BackupCtx> ctx) {
    m_backup_name = backup_name;
    m_ctx = ctx;
}

AsyncTask::~AsyncTask() {
}

LocalCreateTask::LocalCreateTask(const string& backup_name,
        shared_ptr<BackupCtx> ctx) : AsyncTask(backup_name, ctx) {
    m_task_name.append(backup_name);
    m_task_name.append("_create_backup");
    m_task_type   = BACKUP_CREATE;
    m_task_status = TASK_CREATE;
}

LocalCreateTask::~LocalCreateTask() {
}

StatusCode LocalCreateTask::do_full_backup() {
    StatusCode ret_code = StatusCode::sOk;
    off_t  cur_pos = 0;
    off_t  end_pos = m_ctx->vol_size();
    size_t chunk_size = BACKUP_BLOCK_SIZE;
    char* chunk_buf = new char[chunk_size];
    assert(chunk_buf != nullptr);
    string cur_backup = m_backup_name;

    while (cur_pos < end_pos) {
        chunk_size =((end_pos-cur_pos) > BACKUP_BLOCK_SIZE) ?  \
                      BACKUP_BLOCK_SIZE : (end_pos-cur_pos);
        /*read current snapshot*/
        string cur_snap = backup_to_snap_name(cur_backup);
        ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(),
                                   cur_snap, chunk_buf, chunk_size, cur_pos);
        assert(ret_code == StatusCode::sOk);
        /*spawn block object*/
        block_t cur_blk_no = (cur_pos / BACKUP_BLOCK_SIZE);
        backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
        backup_object_t cur_blk_obj = spawn_backup_object_name(
                m_ctx->vol_name(), cur_backup_id, cur_blk_no);
        /*store backup data to block store in object*/
        int write_ret = m_ctx->block_store()->write(cur_blk_obj, chunk_buf,
                                                    chunk_size, 0);
        assert(write_ret == 0);

        /*update backup block map*/
        auto cur_backup_block_map_it = m_ctx->cur_blocks_map().find(cur_backup_id);
        assert(cur_backup_block_map_it != m_ctx->cur_blocks_map().end());
        map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
        cur_backup_block_map.insert({cur_blk_no, cur_blk_obj});
        cur_pos += chunk_size;
    }

    if (chunk_buf) {
        delete [] chunk_buf;
    }
    return ret_code;
}

StatusCode LocalCreateTask::do_incr_backup() {
    StatusCode ret_code = StatusCode::sOk;
    string cur_backup   = m_backup_name;
    string prev_backup  = m_ctx->get_prev_backup(cur_backup);
    if (prev_backup.empty()) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no previous backup";
        return StatusCode::sBackupNotExist;
    }

    string pre_snap = backup_to_snap_name(prev_backup);
    string cur_snap = backup_to_snap_name(cur_backup);
    if (!m_ctx->is_snapshot_valid(pre_snap) ||
        !m_ctx->is_snapshot_valid(cur_snap)) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no snapshot";
        return StatusCode::sBackupNotExist;
    }

    /*todo modify as stream interface*/
    /*1. diff cur and prev snapshot*/
    vector<DiffBlocks> diff_blocks;
    ret_code = m_ctx->snap_client()->DiffSnapshot(m_ctx->vol_name(), pre_snap,
                                                  cur_snap, diff_blocks);
    assert(ret_code == StatusCode::sOk);

    /*2. read diff data(current snapshot and diff region)*/
    char* buf = new char[COW_BLOCK_SIZE];
    for (auto diff_block : diff_blocks) {
        uint64_t block_size = diff_block.block_size();
        for (int i = 0; i < block_size; i++) {
            uint64_t diff_block_no = diff_block.block(i).blk_no();
            bool diff_block_zero = diff_block.block(i).blk_zero();
            std::string diff_block_url = diff_block.block(i).blk_url();
            off_t diff_block_off = diff_block_no * COW_BLOCK_SIZE;
            size_t diff_block_size = COW_BLOCK_SIZE;
            
            ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(),
                            cur_snap, buf, diff_block_size, diff_block_off);
            assert(ret_code == StatusCode::sOk);

            /*3. append backup meta(block, object) and (object, backup_ref)*/
            block_t cur_backup_blk_no = (diff_block_off / BACKUP_BLOCK_SIZE);
            backupid_t cur_backup_id  = m_ctx->get_backup_id(cur_backup);
            backup_object_t cur_backup_blk_obj = spawn_backup_object_name(
                    m_ctx->vol_name(), cur_backup_id, cur_backup_blk_no);

            /*store backup data to block store in object*/
            size_t write_size = diff_block_size;
            /*snapshot cow block  unit diff from backup block unit*/
            off_t  write_off = (diff_block_off % BACKUP_BLOCK_SIZE);
            int write_ret = m_ctx->block_store()->write(cur_backup_blk_obj,
                                                 buf, write_size, write_off);
            assert(write_ret == 0);

            /*update backup block map*/
            auto cur_backup_block_map_it = m_ctx->cur_blocks_map().find(cur_backup_id);
            assert(cur_backup_block_map_it != m_ctx->cur_blocks_map().end());
            map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
            cur_backup_block_map.insert({cur_backup_blk_no, cur_backup_blk_obj});
        }
    }

    if (buf) {
        delete [] buf;
    }
    return ret_code;
}

bool LocalCreateTask::ready() {
    /*query snapshot of current backup whether created*/
    string cur_backup_snap = backup_to_snap_name(m_backup_name);
    SnapStatus cur_backup_snap_status;

    LOG_INFO << "local create ready";
    m_ctx->snap_client()->QuerySnapshot(m_ctx->vol_name(), cur_backup_snap,
                                        cur_backup_snap_status);
    if (cur_backup_snap_status != SnapStatus::SNAP_CREATED) {
        LOG_ERROR << " snapshot not ready";
        return false;
    }

    LOG_INFO << "local create now ready";
    m_task_status = TASK_READY;
    return true;
}

void LocalCreateTask::work() {
    string cur_backup = m_backup_name;
    auto it = m_ctx->cur_backups_map().find(cur_backup);
    if (it == m_ctx->cur_backups_map().end()) {
        LOG_ERROR << "cur_backup:" << cur_backup<< " no exist";
        m_task_status = TASK_ERROR;
        return;
    }

    m_task_status = TASK_RUN;

    /*generate backup id*/
    backupid_t backup_id = m_ctx->spawn_backup_id();
    /*update backup attr*/
    it->second.backup_id = backup_id;
    /*prepare backup block map*/
    map<block_t, backup_object_t> block_map;
    m_ctx->cur_blocks_map().insert({backup_id, block_map});

    /*do backup and update backup block map*/
    if (it->second.backup_mode == BackupMode::BACKUP_FULL) {
        do_full_backup();
    } else if (it->second.backup_mode == BackupMode::BACKUP_INCR) {
        do_incr_backup();
    } else {
    }

    it->second.backup_status = BackupStatus::BACKUP_AVAILABLE;

    /*db persist*/
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey = spawn_latest_backup_id_key();
    string pval = to_string(m_ctx->latest_backup_id());
    transaction->put(pkey, pval);
    pkey = spawn_backup_attr_map_key(cur_backup);
    pval = spawn_backup_attr_map_val(it->second);
    transaction->put(pkey, pval);
    for (auto block : block_map) {
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        pval = block.second;
        transaction->put(pkey, pval);
    }
    m_ctx->index_store()->submit_transaction(transaction);

    /*delete snapshot of prev backup*/
    string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if (!prev_backup.empty() &&
         m_ctx->get_backup_mode(prev_backup) == BackupMode::BACKUP_INCR) {
        string prev_snap = backup_to_snap_name(prev_backup);
        if (!prev_snap.empty()) {
            StatusCode ret = m_ctx->snap_client()->DeleteSnapshot(
                                             m_ctx->vol_name(), prev_snap);
            assert(ret == StatusCode::sOk);
        }
    }
    m_task_status = TASK_DONE;
    m_ctx->trace();
}

RemoteCreateTask::RemoteCreateTask(const string& backup_name,
        shared_ptr<BackupCtx> ctx) : LocalCreateTask(backup_name, ctx) {
    ClientContext* rpc_ctx = new ClientContext;
    m_remote_stream = NetSender::instance().create_stream(rpc_ctx);
    if (m_remote_stream == nullptr) {
        LOG_ERROR << "remote rpc stream failed";
    }
}

RemoteCreateTask::~RemoteCreateTask() {
}

StatusCode RemoteCreateTask::do_full_backup() {
    StatusCode ret_code = StatusCode::sOk;
    LOG_INFO << " remote create start";
    /*notify start create backup meta on remote site*/
    ret_code = remote_create_start();
    if (ret_code) {
        return ret_code;
    }

    LOG_INFO << " remote create upload";
    off_t  cur_pos = 0;
    off_t  end_pos = m_ctx->vol_size();
    size_t chunk_size = BACKUP_BLOCK_SIZE;
    char* chunk_buf = new char[chunk_size];
    assert(chunk_buf != nullptr);
    string cur_backup = m_backup_name;
    while (cur_pos < end_pos) {
        chunk_size =((end_pos-cur_pos) > BACKUP_BLOCK_SIZE) ?  \
                    BACKUP_BLOCK_SIZE : (end_pos-cur_pos);
        /*read current snapshot*/
        string cur_snap = backup_to_snap_name(cur_backup);
        ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(),
                            cur_snap, chunk_buf, chunk_size, cur_pos);
        assert(ret_code == StatusCode::sOk);
        /*spawn block object*/
        block_t cur_blk_no = (cur_pos / BACKUP_BLOCK_SIZE);
        off_t   cur_blk_off =(cur_pos % BACKUP_BLOCK_SIZE);
        /*upload backup data to remote site*/
        ret_code = remote_create_upload(cur_blk_no, cur_blk_off,
                                        chunk_buf, chunk_size);
        assert(ret_code == StatusCode::sOk);
        cur_pos += chunk_size;
    }

    if (chunk_buf) {
        delete [] chunk_buf;
    }

    LOG_INFO << " remote create end";
    /*notify stop create backup meta on remote site*/
    ret_code = remote_create_end();
    return ret_code;
}

StatusCode RemoteCreateTask::do_incr_backup() {
    StatusCode ret_code = StatusCode::sOk;
    string cur_backup = m_backup_name;
    string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if (prev_backup.empty()) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no previous backup";
        return StatusCode::sBackupNotExist;
    }
    string pre_snap = backup_to_snap_name(prev_backup);
    string cur_snap = backup_to_snap_name(cur_backup);
    if (!m_ctx->is_snapshot_valid(pre_snap) ||
        !m_ctx->is_snapshot_valid(cur_snap)) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no snapshot";
        return StatusCode::sBackupNotExist;
    }

    /*notify start create backup meta on remote site*/
    ret_code = remote_create_start();
    if (ret_code) {
        return ret_code;
    }

    /*todo modify as stream interface diff cur and prev snapshot*/
    vector<DiffBlocks> diff_blocks;
    ret_code = m_ctx->snap_client()->DiffSnapshot(m_ctx->vol_name(),
                                         pre_snap, cur_snap, diff_blocks);
    assert(ret_code == StatusCode::sOk);

    /*read diff data(current snapshot and diff region)*/
    size_t chunk_size = COW_BLOCK_SIZE;
    char* chunk_buf = new char[COW_BLOCK_SIZE];
    for (auto diff_block : diff_blocks) {
        uint64_t block_num = diff_block.block_size();
        for (int i = 0; i < block_num; i++) {
            uint64_t diff_block_no = diff_block.block(i).blk_no();
            bool diff_block_zero = diff_block.block(i).blk_zero();
            std::string diff_block_url = diff_block.block(i).blk_url();
            off_t diff_block_off = diff_block_no * COW_BLOCK_SIZE;

            ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(),
                                                          cur_snap,
                                                          chunk_buf,
                                                          chunk_size,
                                                          diff_block_off);
            assert(ret_code == StatusCode::sOk);

            /*3. append backup meta(block, object) and (object, backup_ref)*/
            block_t cur_blk_no  = (diff_block_off / BACKUP_BLOCK_SIZE);
            off_t   cur_blk_off = (diff_block_off % BACKUP_BLOCK_SIZE);
            ret_code = remote_create_upload(cur_blk_no, cur_blk_off,
                                            chunk_buf, chunk_size);
            assert(ret_code == StatusCode::sOk);
        }
    }

    if (chunk_buf) {
        delete [] chunk_buf;
    }

    /*notify stop create backup meta on remote site*/
    ret_code = remote_create_end();
    return ret_code;
}

bool RemoteCreateTask::ready() {
    /*query snapshot of current backup whether created*/
    LOG_ERROR << " remote create ready ";
    string cur_backup_snap = backup_to_snap_name(m_backup_name);
    SnapStatus cur_backup_snap_status;

    m_ctx->snap_client()->QuerySnapshot(m_ctx->vol_name(), cur_backup_snap,
                                        cur_backup_snap_status);
    if (cur_backup_snap_status != SnapStatus::SNAP_CREATED) {
        LOG_ERROR << " snapshot not ready";
        return false;
    }

    LOG_ERROR << " remote create ready now";
    /*todo check network connection to remote site available*/
    m_task_status = TASK_READY;
    return true;
}

StatusCode RemoteCreateTask::remote_create_start() {
    RemoteBackupStartReq start_req;

    LOG_INFO << "send remote create start req 1";
    start_req.set_vol_name(m_ctx->vol_name());
    start_req.set_vol_size(m_ctx->vol_size());
    start_req.set_backup_name(m_backup_name);
    start_req.set_backup_mode(m_ctx->get_backup_mode(m_backup_name));
    start_req.set_backup_type(m_ctx->get_backup_type(m_backup_name));

    string start_req_buf;
    start_req.SerializeToString(&start_req_buf);

    TransferRequest transfer_req;
    transfer_req.set_type(MessageType::REMOTE_BACKUP_CREATE_START);
    transfer_req.set_data(start_req_buf.c_str(), start_req_buf.length());

    LOG_INFO << "send remote create start req 2";
    if (!m_remote_stream->Write(transfer_req)) {
        LOG_ERROR << "send remote create start req failed";
        return StatusCode::sInternalError;
    }

    LOG_INFO << "send remote create start req 3";
    TransferResponse transfer_res;
    if (!m_remote_stream->Read(&transfer_res)) {
        LOG_ERROR << "recv remote create start res failed";
        return StatusCode::sInternalError;
    }

    LOG_INFO << "send remote create start req ok";
    return StatusCode::sOk;
}

StatusCode RemoteCreateTask::remote_create_upload(block_t blk_no, off_t blk_off,
                                    char* blk_data, size_t blk_data_len) {
    UploadDataReq upload_req;
    upload_req.set_vol_name(m_ctx->vol_name());
    upload_req.set_backup_name(m_backup_name);
    upload_req.set_blk_no(blk_no);
    upload_req.set_blk_off(blk_off);
    upload_req.set_blk_data(blk_data, blk_data_len);

    string upload_req_buf;
    upload_req.SerializeToString(&upload_req_buf);

    TransferRequest transfer_req;
    transfer_req.set_type(MessageType::REMOTE_BACKUP_UPLOAD_DATA);
    transfer_req.set_data(upload_req_buf.c_str(), upload_req_buf.length());

    if (!m_remote_stream->Write(transfer_req)) {
        LOG_ERROR << "send upload data req failed";
        return StatusCode::sInternalError;
    }
    return StatusCode::sOk;
}

StatusCode RemoteCreateTask::remote_create_end() {
    RemoteBackupEndReq end_req;
    end_req.set_vol_name(m_ctx->vol_name());
    end_req.set_backup_name(m_backup_name);

    string end_req_buf;
    end_req.SerializeToString(&end_req_buf);

    TransferRequest transfer_req;
    transfer_req.set_type(MessageType::REMOTE_BACKUP_CREATE_END);
    transfer_req.set_data(end_req_buf.c_str(), end_req_buf.length());

    if (!m_remote_stream->Write(transfer_req)) {
        LOG_ERROR << "send remote create end req failed";
        return StatusCode::sInternalError;
    }

    TransferResponse transfer_res;
    if (!m_remote_stream->Read(&transfer_res)) {
        LOG_ERROR << "recv remote create end res failed";
        return StatusCode::sInternalError;
    }
    return StatusCode::sOk;
}

LocalDeleteTask::LocalDeleteTask(const string& backup_name,
            shared_ptr<BackupCtx> ctx) : AsyncTask(backup_name, ctx) {
    m_task_name.append(backup_name);
    m_task_name.append("_delete_backup");

    m_task_type = BACKUP_DELETE;
    m_task_status = TASK_CREATE;
}

LocalDeleteTask::~LocalDeleteTask() {
}

StatusCode LocalDeleteTask::do_delete_backup(const string& cur_backup) {
    backupid_t backup_id = m_ctx->get_backup_id(cur_backup);
    assert(backup_id != -1);

    auto backup_it = m_ctx->cur_blocks_map().find(backup_id);
    assert(backup_it != m_ctx->cur_blocks_map().end());

    /*delete backup object*/
    auto block_map = backup_it->second;
    for (auto block_it : block_map) {
        m_ctx->block_store()->remove(block_it.second);
    }

    /*db persist*/
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey;
    for (auto block : block_map) {
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        transaction->del(pkey);
    }
    pkey = spawn_backup_attr_map_key(cur_backup);
    transaction->del(pkey);
    m_ctx->index_store()->submit_transaction(transaction);

    /*delete backup meta in memory*/
    block_map.clear();
    m_ctx->cur_blocks_map().erase(backup_id);
    m_ctx->cur_backups_map().erase(cur_backup);
    return StatusCode::sOk;
}

StatusCode LocalDeleteTask::do_merge_backup(const string& cur_backup,
                                            const string& next_backup) {
    backupid_t cur_backup_id  = m_ctx->get_backup_id(cur_backup);
    backupid_t next_backup_id = m_ctx->get_backup_id(next_backup);
    assert(cur_backup_id != -1 && next_backup_id != -1);

    auto cur_backup_it = m_ctx->cur_blocks_map().find(cur_backup_id);
    auto next_backup_it = m_ctx->cur_blocks_map().find(next_backup_id);
    assert(cur_backup_it != m_ctx->cur_blocks_map().end());
    assert(next_backup_it != m_ctx->cur_blocks_map().end());
    auto& cur_blocks_map  = cur_backup_it->second;
    auto& next_block_map = next_backup_it->second;
    for (auto cur_block_it : cur_blocks_map) {
        auto ret = next_block_map.insert({cur_block_it.first, cur_block_it.second});
        if (!ret.second) {
            /*backup block already in next bakcup, delete attached object*/
            m_ctx->block_store()->remove(cur_block_it.second);
        }
    }

     /*db persist*/
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey;
    string pval;
    for (auto block : cur_blocks_map) {
        pkey = spawn_backup_block_map_key(cur_backup_id, block.first);
        transaction->del(pkey);
    }
    pkey = spawn_backup_attr_map_key(cur_backup);
    transaction->del(pkey);
    for (auto block : next_block_map) {
       pkey = spawn_backup_block_map_key(next_backup_id, block.first);
       pval = block.second;
       transaction->put(pkey, pval);
    }
    m_ctx->index_store()->submit_transaction(transaction);

    /*delete backup meta in memory*/
    cur_blocks_map.clear();
    m_ctx->cur_blocks_map().erase(cur_backup_id);
    m_ctx->cur_backups_map().erase(cur_backup);

    return StatusCode::sOk;
}

bool LocalDeleteTask::ready() {
    m_task_status = TASK_READY;
    return true;
}

void LocalDeleteTask::work() {
    m_task_status = TASK_RUN;
   /* delete backup snapshot
    * snapshot may deleted repeated, no matter
    */
    string cur_backup = m_backup_name;
    string backup_snap = backup_to_snap_name(cur_backup);
    m_ctx->snap_client()->DeleteSnapshot(m_ctx->vol_name(), backup_snap);

    BackupMode backup_mode = m_ctx->get_backup_mode(cur_backup);

    if (backup_mode == BackupMode::BACKUP_FULL) {
        /*can directly delete */
        do_delete_backup(cur_backup);
    } else if (backup_mode == BackupMode::BACKUP_INCR) {
        string next_backup = m_ctx->get_next_backup(cur_backup);
        if (next_backup.empty()) {
            do_delete_backup(cur_backup);
        } else {
            BackupMode next_backup_mode = m_ctx->get_backup_mode(cur_backup);
            if (next_backup_mode == BackupMode::BACKUP_FULL) {
                /*can directly delete*/
                do_delete_backup(cur_backup);
            } else {
                /*merge with next backup and then delete*/
                do_merge_backup(cur_backup, next_backup);
            }
        }
    }

    m_task_status = TASK_DONE;
    m_ctx->trace();
}

RemoteDeleteTask::RemoteDeleteTask(const string& backup_name,
            shared_ptr<BackupCtx> ctx) :LocalDeleteTask(backup_name, ctx) {
    ClientContext* rpc_ctx = new ClientContext;
    m_remote_stream = NetSender::instance().create_stream(rpc_ctx);
    if (m_remote_stream == nullptr) {
        LOG_ERROR << "remote rpc stream failed";
    }
}

RemoteDeleteTask::~RemoteDeleteTask() {
}

void RemoteDeleteTask::work() {
    /*sent backup delete to remote site*/
    if (remote_delete()) {
        LOG_ERROR << " remote delete error";
    }

    LocalDeleteTask::work();
}

StatusCode RemoteDeleteTask::remote_delete() {
    RemoteBackupDeleteReq del_req;
    del_req.set_vol_name(m_ctx->vol_name());
    del_req.set_backup_name(m_backup_name);

    string del_req_buf;
    del_req.SerializeToString(&del_req_buf);

    TransferRequest transfer_req;
    transfer_req.set_type(MessageType::REMOTE_BACKUP_DELETE);
    transfer_req.set_data(del_req_buf.c_str(), del_req_buf.length());
    if (!m_remote_stream->Write(transfer_req)) {
        LOG_ERROR << "send remote delete req failed";
        return StatusCode::sInternalError;
    }

    TransferResponse transfer_res;
    if (!m_remote_stream->Read(&transfer_res)) {
        LOG_ERROR << "recv remote delete res failed";
        return StatusCode::sInternalError;
    }

    LOG_ERROR << "send remote delete req ok";
    return StatusCode::sOk;
}
