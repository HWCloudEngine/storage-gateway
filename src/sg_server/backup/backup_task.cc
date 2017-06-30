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

AsyncTask::AsyncTask(const std::string& backup_name, shared_ptr<BackupCtx> ctx) {
    m_backup_name = backup_name;
    m_ctx = ctx;
}

AsyncTask::~AsyncTask() {
}

LocalCreateTask::LocalCreateTask(const std::string& backup_name, std::shared_ptr<BackupCtx> ctx)
    : AsyncTask(backup_name, ctx) {
    m_task_name.append(backup_name);
    m_task_name.append("_create_backup");
    m_task_type   = BACKUP_CREATE;
    m_task_status = TASK_CREATE;
}

LocalCreateTask::~LocalCreateTask() {
}

StatusCode LocalCreateTask::do_full_backup() {
    StatusCode ret = StatusCode::sOk;
    off_t cur_pos = 0;
    off_t end_pos = m_ctx->vol_size();
    size_t chunk_size = BACKUP_BLOCK_SIZE;
    char* chunk_buf = new char[chunk_size];
    assert(chunk_buf != nullptr);
    std::string cur_backup = m_backup_name;
    while (cur_pos < end_pos) {
        chunk_size =((end_pos-cur_pos) > BACKUP_BLOCK_SIZE) ?  BACKUP_BLOCK_SIZE : (end_pos-cur_pos);
        /*read current snapshot*/
        std::string cur_snap = backup_to_snap_name(cur_backup);
        ret = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(), cur_snap, chunk_buf, chunk_size, cur_pos);
        if (ret) {
            LOG_ERROR << "read snapshot failed ret:" << ret;
            return ret;
        }
        /*backup block*/
        backup_block_t cur_block;
        cur_block.vol_name = m_ctx->vol_name();
        cur_block.backup_id = m_ctx->get_backup_id(cur_backup);
        cur_block.block_no = (cur_pos / BACKUP_BLOCK_SIZE);
        cur_block.block_zero = mem_is_zero(chunk_buf, chunk_size);
        cur_block.block_url = spawn_backup_block_url(cur_block);
        /*store backup data to block store in object*/
        if (!cur_block.block_zero) {
            int write_ret = m_ctx->block_store()->write(cur_block.block_url, chunk_buf, chunk_size, 0);
            if (write_ret != 0) {
                LOG_ERROR << "block store write failed ret:" << write_ret;
                return StatusCode::sInternalError;
            }
        }
        m_ctx->index_store()->db_put(cur_block.key(), cur_block.encode());
        /*update backup block map*/
        cur_pos += chunk_size;
    }
    if (chunk_buf) {
        delete [] chunk_buf;
    }
    return ret;
}

StatusCode LocalCreateTask::do_incr_backup() {
    StatusCode ret_code = StatusCode::sOk;
    std::string cur_backup = m_backup_name;
    std::string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if (cur_backup.empty() || prev_backup.empty()) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no previous backup";
        return StatusCode::sBackupNotExist;
    }
    std::string pre_snap = backup_to_snap_name(prev_backup);
    std::string cur_snap = backup_to_snap_name(cur_backup);
    if (!m_ctx->is_snapshot_valid(pre_snap) ||
        !m_ctx->is_snapshot_valid(cur_snap)) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no snapshot";
        return StatusCode::sBackupNotExist;
    }
    vector<DiffBlocks> diff_blocks;
    ret_code = m_ctx->snap_client()->DiffSnapshot(m_ctx->vol_name(), pre_snap,
                                                  cur_snap, diff_blocks);
    if (ret_code) {
        LOG_ERROR << "incr backup:" << cur_backup<< " diff snapshot failed";
        return ret_code;
    }
    if (diff_blocks.empty()) {
        LOG_ERROR << "incr backup:" << cur_backup<< " diff snapshot empty";
        return StatusCode::sOk;
    }
    /*read diff data(current snapshot and diff region)*/
    char* buf = new char[COW_BLOCK_SIZE];
    assert(buf != nullptr);
    for (auto diff_block : diff_blocks) {
        uint64_t block_num = diff_block.block_size();
        for (int i = 0; i < block_num; i++) {
            uint64_t block_no = diff_block.block(i).blk_no();
            bool block_zero = diff_block.block(i).blk_zero();
            std::string block_url = diff_block.block(i).blk_url();
            off_t block_off = block_no * COW_BLOCK_SIZE;
            size_t block_size = COW_BLOCK_SIZE;
            ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(), cur_snap, buf, block_size, block_off);
            if (ret_code) {
                LOG_ERROR << "incr backup:" << cur_backup<< " read snapshot failed";
                return ret_code;
            }
            /*append backup meta(block, object)*/
            backup_block_t cur_block;
            cur_block.vol_name = m_ctx->vol_name();
            cur_block.backup_id = m_ctx->get_backup_id(cur_backup);
            cur_block.block_no = block_no;
            cur_block.block_zero = mem_is_zero(buf, block_size);
            cur_block.block_url = spawn_backup_block_url(cur_block);
            /*store backup data to block store in object*/
            if (!cur_block.block_zero) {
                size_t write_size = block_size;
                off_t  write_off = (block_off % BACKUP_BLOCK_SIZE);
                int write_ret = m_ctx->block_store()->write(cur_block.block_url, buf, write_size, write_off);
                assert(write_ret == 0);
            }
            /*update backup block map*/
            m_ctx->index_store()->db_put(cur_block.key(), cur_block.encode());
        }
    }
    if (buf) {
        delete [] buf;
    }
    return ret_code;
}

bool LocalCreateTask::ready() {
    /*query snapshot of current backup whether created*/
    std::string cur_backup_snap = backup_to_snap_name(m_backup_name);
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
    std::string cur_backup = m_backup_name;
    if (!m_ctx->is_backup_exist(cur_backup)) {
        LOG_ERROR << "cur_backup:" << cur_backup<< " no exist";
        m_task_status = TASK_ERROR;
        return;
    }
    m_task_status = TASK_RUN;
    auto cur_backup_mode = m_ctx->get_backup_mode(cur_backup);
    /*do backup and update backup block map*/
    if (cur_backup_mode == BackupMode::BACKUP_FULL) {
        do_full_backup();
    } else if (cur_backup_mode == BackupMode::BACKUP_INCR) {
        do_incr_backup();
    } else {
    }
    backup_t current;
    current.vol_name = m_ctx->vol_name();
    current.backup_id = m_ctx->get_backup_id(cur_backup);
    IndexStore::IndexIterator it = m_ctx->index_store()->db_iterator();
    it->seek_to_first(current.key());
    if (!it->valid()) {
        LOG_ERROR << "cur_backup:" << cur_backup<< " no exist";
        return;
    }
    current.decode(it->value());
    current.backup_status = BackupStatus::BACKUP_AVAILABLE;
    m_ctx->index_store()->db_put(current.key(), current.encode());
    /*delete snapshot of prev backup*/
    std::string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if (!prev_backup.empty() & m_ctx->get_backup_mode(prev_backup) == BackupMode::BACKUP_INCR) {
        std::string prev_snap = backup_to_snap_name(prev_backup);
        if (!prev_snap.empty()) {
            StatusCode ret = m_ctx->snap_client()->DeleteSnapshot(m_ctx->vol_name(), prev_snap);
            assert(ret == StatusCode::sOk);
        }
    }
    m_task_status = TASK_DONE;
    m_ctx->trace();
}

RemoteCreateTask::RemoteCreateTask(const std::string& backup_name, std::shared_ptr<BackupCtx> ctx)
     : LocalCreateTask(backup_name, ctx) {
    ClientContext* rpc_ctx = new ClientContext;
    assert(rpc_ctx != nullptr);
    m_remote_stream = NetSender::instance().create_stream(rpc_ctx);
    if (m_remote_stream == nullptr) {
        LOG_ERROR << "remote rpc stream failed";
    }
}

RemoteCreateTask::~RemoteCreateTask() {
}

StatusCode RemoteCreateTask::do_full_backup() {
    StatusCode ret_code = StatusCode::sOk;
    LOG_INFO << "remote create start";
    /*notify start create backup meta on remote site*/
    ret_code = remote_create_start();
    if (ret_code) {
        return ret_code;
    }
    LOG_INFO << "remote create upload";
    off_t cur_pos = 0;
    off_t end_pos = m_ctx->vol_size();
    size_t chunk_size = BACKUP_BLOCK_SIZE;
    char* chunk_buf = new char[chunk_size];
    assert(chunk_buf != nullptr);
    std::string cur_backup = m_backup_name;
    while (cur_pos < end_pos) {
        chunk_size =((end_pos-cur_pos) > BACKUP_BLOCK_SIZE) ? BACKUP_BLOCK_SIZE : (end_pos-cur_pos);
        /*read current snapshot*/
        std::string cur_snap = backup_to_snap_name(cur_backup);
        ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(), cur_snap, chunk_buf, chunk_size, cur_pos);
        assert(ret_code == StatusCode::sOk);
        /*spawn block object*/
        block_t cur_blk_no = (cur_pos / BACKUP_BLOCK_SIZE);
        off_t   cur_blk_off =(cur_pos % BACKUP_BLOCK_SIZE);
        /*upload backup data to remote site*/
        ret_code = remote_create_upload(cur_blk_no, cur_blk_off, chunk_buf, chunk_size);
        assert(ret_code == StatusCode::sOk);
        cur_pos += chunk_size;
    }
    if (chunk_buf) {
        delete [] chunk_buf;
    }
    LOG_INFO << "remote create end";
    /*notify stop create backup meta on remote site*/
    ret_code = remote_create_end();
    return ret_code;
}

StatusCode RemoteCreateTask::do_incr_backup() {
    StatusCode ret_code = StatusCode::sOk;
    std::string cur_backup = m_backup_name;
    std::string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if (prev_backup.empty()) {
        LOG_ERROR << "incr backup:" << cur_backup<< "has no previous backup";
        return StatusCode::sBackupNotExist;
    }
    std::string pre_snap = backup_to_snap_name(prev_backup);
    std::string cur_snap = backup_to_snap_name(cur_backup);
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
    ret_code = m_ctx->snap_client()->DiffSnapshot(m_ctx->vol_name(), pre_snap, cur_snap, diff_blocks);
    assert(ret_code == StatusCode::sOk);
    /*read diff data(current snapshot and diff region)*/
    size_t chunk_size = COW_BLOCK_SIZE;
    char* chunk_buf = new char[COW_BLOCK_SIZE];
    assert(chunk_buf != nullptr);
    for (auto diff_block : diff_blocks) {
        uint64_t block_num = diff_block.block_size();
        for (int i = 0; i < block_num; i++) {
            uint64_t block_no = diff_block.block(i).blk_no();
            bool block_zero = diff_block.block(i).blk_zero();
            std::string block_url = diff_block.block(i).blk_url();
            off_t block_off = block_no * COW_BLOCK_SIZE;
            ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(), cur_snap, chunk_buf, chunk_size, block_off);
            assert(ret_code == StatusCode::sOk);

            /*append backup meta(block, object) and (object, backup_ref)*/
            block_t cur_blk_no  = (block_off / BACKUP_BLOCK_SIZE);
            off_t   cur_blk_off = (block_off % BACKUP_BLOCK_SIZE);
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
    LOG_ERROR << "remote create ready";
    string cur_backup_snap = backup_to_snap_name(m_backup_name);
    SnapStatus cur_backup_snap_status;
    m_ctx->snap_client()->QuerySnapshot(m_ctx->vol_name(), cur_backup_snap,
                                        cur_backup_snap_status);
    if (cur_backup_snap_status != SnapStatus::SNAP_CREATED) {
        LOG_ERROR << "snapshot not ready";
        return false;
    }
    LOG_ERROR << "remote create ready now";
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
    bool block_zero = mem_is_zero(blk_data, blk_data_len);
    upload_req.set_blk_zero(block_zero);
    if (!block_zero) {
        upload_req.set_blk_data(blk_data, blk_data_len);
    }
    std::string upload_req_buf;
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

LocalDeleteTask::LocalDeleteTask(const string& backup_name, shared_ptr<BackupCtx> ctx) : AsyncTask(backup_name, ctx) {
    m_task_name.append(backup_name);
    m_task_name.append("_delete_backup");
    m_task_type = BACKUP_DELETE;
    m_task_status = TASK_CREATE;
}

LocalDeleteTask::~LocalDeleteTask() {
}

StatusCode LocalDeleteTask::do_delete_backup(const std::string& cur_backup) {
    backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
    if (-1 == cur_backup_id) {
        LOG_ERROR << "cur backup:" << cur_backup << " no exist";
        return StatusCode::sInternalError;
    }
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    IndexStore::IndexIterator it = m_ctx->index_store()->db_iterator();
    backup_t current;
    current.vol_name = m_ctx->vol_name();
    current.backup_id = cur_backup_id;
    it->seek_to_first(current.key());
    if (!it->valid()) {
        LOG_ERROR << "cur backup:" << cur_backup << " no exist";
        return StatusCode::sInternalError;
    }
    current.decode(it->value());
    transaction->del(current.key());

    IndexStore::IndexIterator blk_it = m_ctx->index_store()->db_iterator();
    backup_block_t pivot;
    pivot.vol_name = m_ctx->vol_name();
    pivot.backup_id = cur_backup_id;
    blk_it->seek_to_first(pivot.prefix());
    for (; blk_it->valid()&&(-1 != blk_it->key().find(pivot.prefix())); blk_it->next()) {
        backup_block_t cur_block;
        cur_block.decode(blk_it->value());
        if (!cur_block.block_zero) {
            m_ctx->block_store()->remove(cur_block.block_url);
        }
        transaction->del(cur_block.key());
    }
    m_ctx->index_store()->submit_transaction(transaction);
    return StatusCode::sOk;
}

StatusCode LocalDeleteTask::do_merge_backup(const std::string& cur_backup,
                                            const std::string& next_backup) {
    backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
    backupid_t next_backup_id = m_ctx->get_backup_id(next_backup);
    if (cur_backup_id == -1 || next_backup_id == -1) {
        LOG_ERROR << "can not merge backup cur:" << cur_backup_id << " next:" << next_backup_id;
        return StatusCode::sInternalError;
    }
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    backup_t current;
    current.vol_name = m_ctx->vol_name();
    current.backup_id = cur_backup_id;
    transaction->del(current.key());
    
    IndexStore::IndexIterator cur_blk_it = m_ctx->index_store()->db_iterator();
    backup_block_t cur_pivot;
    cur_pivot.vol_name = m_ctx->vol_name();
    cur_pivot.backup_id = cur_backup_id;
    cur_blk_it->seek_to_first(cur_pivot.prefix());
    for (; cur_blk_it->valid()&&(-1 != cur_blk_it->key().find(cur_pivot.prefix())); cur_blk_it->next()) {
        backup_block_t cur_block;
        cur_block.decode(cur_blk_it->value());
        transaction->del(cur_block.key());
        cur_block.backup_id = next_backup_id;
        transaction->put(cur_block.key(), cur_block.encode());
    }
    m_ctx->index_store()->submit_transaction(transaction);
    return StatusCode::sOk;
}

bool LocalDeleteTask::ready() {
    m_task_status = TASK_READY;
    return true;
}

void LocalDeleteTask::work() {
    m_task_status = TASK_RUN;
   /* no matter snapshot may deleted repeated, no matter */
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

RemoteDeleteTask::RemoteDeleteTask(const string& backup_name, shared_ptr<BackupCtx> ctx)
    : LocalDeleteTask(backup_name, ctx) {
    ClientContext* rpc_ctx = new ClientContext;
    assert(rpc_ctx != nullptr);
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
