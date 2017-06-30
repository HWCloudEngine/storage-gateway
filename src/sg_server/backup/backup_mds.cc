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
#include <cstdlib>
#include <assert.h>
#include <map>
#include "log/log.h"
#include "common/define.h"
#include "common/utils.h"
#include "rpc/transfer.pb.h"
#include "backup_type.h"
#include "backup_ctx.h"
#include "backup_task.h"
#include "backup_mds.h"

using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;
using huawei::proto::transfer::MessageType;

BackupMds::BackupMds(const std::string& vol_name, const size_t& vol_size) {
    m_ctx.reset(new BackupCtx(vol_name, vol_size));
    m_task_schedule_run.store(true);
    m_thread_pool.reset(new ThreadPool(5));
    m_thread_pool->submit(bind(&BackupMds::do_task_schedule, this));
}

BackupMds::~BackupMds() {
    m_ctx.reset();
    m_task_schedule_list.clear();
    m_task_schedule_run.store(false);
    m_thread_pool.reset();
}

StatusCode BackupMds::prepare_create(const std::string& bname, const BackupMode& bmode,
                                     const BackupType& btype, bool on_remote) {
    LOG_INFO << "prepare create vname:" << m_ctx->vol_name() << " bname:" << bname << " on_remote:" << on_remote;
    /*check whether backup exist*/
    if (m_ctx->is_backup_exist(bname)) {
        LOG_ERROR << "bname:" << bname << " already exist";
        return StatusCode::sBackupAlreadyExist;
    }
    /*check whether incr backup allowable*/
    if (!on_remote && (bmode == BackupMode::BACKUP_INCR)) {
        if (!m_ctx->is_incr_backup_allowable()) {
            LOG_ERROR << "bname:" << bname << " failed incr backup disallow";
            return StatusCode::sBackupAlreadyExist;
        }
    }
    /*create backup*/
    backup_t current;
    current.vol_name = m_ctx->vol_name();
    current.backup_mode = bmode;
    current.backup_name = bname;
    current.backup_type = btype;
    current.backup_status = BackupStatus::BACKUP_CREATING;
    current.backup_id = m_ctx->spawn_backup_id();
    m_ctx->index_store()->db_put(current.key(), current.encode());
    LOG_INFO << "prepare create vname:" << m_ctx->vol_name() << " bname:" << bname << " on_remote:" << on_remote << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::prepare_delete(const std::string& bname) {
    LOG_INFO << "prepare delete vname:" << m_ctx->vol_name() << " bname:" << bname;
    /*check whether backup exist*/
    if (!m_ctx->is_backup_exist(bname)) {
        LOG_ERROR << "bname:" << bname <<" not exist";
        return StatusCode::sBackupNotExist;
    }
    /*check whether backup can be delete*/
    if (!m_ctx->is_backup_deletable(bname)) {
        LOG_ERROR << "bname:" << bname <<" can not delete";
        return StatusCode::sBackupCanNotDelete;
    }
    /*delete backup*/
    m_ctx->update_backup_status(bname, BackupStatus::BACKUP_DELETING);
    LOG_INFO << "prepare delete vname:" << m_ctx->vol_name() << " bname:" << bname << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::create_backup(const CreateBackupInReq* req, CreateBackupInAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_option().backup_mode();
    BackupType backup_type = req->backup_option().backup_type();
    LOG_INFO << "create backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " btype:" << backup_type;
    ret = prepare_create(backup_name, backup_mode, backup_type, false);
    if (ret) {
        ack->set_status(ret);
        LOG_ERROR << " prepare create failed";
        return ret;
    }
    std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
    shared_ptr<AsyncTask> create_task;
    if (backup_type == BackupType::BACKUP_LOCAL) {
        /*local*/
        create_task.reset(new LocalCreateTask(backup_name, m_ctx));
    } else {
        /*remote*/
        create_task.reset(new RemoteCreateTask(backup_name, m_ctx));
    }
    m_task_schedule_list.push_back(create_task);
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "create backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " btype:" << backup_type << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::list_backup(const ListBackupInReq* req, ListBackupInAck* ack) {
    std::string vol_name = req->vol_name();
    LOG_INFO << "list backup vname:" << m_ctx->vol_name();
    IndexStore::IndexIterator it = m_ctx->index_store()->db_iterator();
    it->seek_to_first(backup_prefix);
    for (; it->valid()&&(-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        ack->add_backup_name(current.backup_name);
    }
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "list backup vname:" << m_ctx->vol_name() << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::get_backup(const GetBackupInReq* req, GetBackupInAck* ack) {
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    LOG_INFO << "get backup vname:" << vol_name << " bname:" << backup_name;
    if (!m_ctx->is_backup_exist(backup_name)) {
        ack->set_status(StatusCode::sOk);
        ack->set_backup_status(BackupStatus::BACKUP_DELETED);
        LOG_INFO << "get backup vname:" << vol_name << " bname:" << backup_name << " failed not exist";
        return StatusCode::sOk;
    }
    BackupStatus backup_status = m_ctx->get_backup_status(backup_name);
    ack->set_status(StatusCode::sOk);
    ack->set_backup_status(backup_status);
    LOG_INFO << "get backup vname:" << vol_name << " bname:" << backup_name << " bstatus:" << backup_status << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::delete_backup(const DeleteBackupInReq* req, DeleteBackupInAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupType backup_type = m_ctx->get_backup_type(backup_name);
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name;
    ret = prepare_delete(backup_name);
    if (ret) {
        ack->set_status(ret);
        LOG_ERROR << "prepare delete failed";
        return ret;
    }
    std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
    shared_ptr<AsyncTask> delete_task;
    if (backup_type == BackupType::BACKUP_LOCAL) {
        /*local */
        delete_task.reset(new LocalDeleteTask(backup_name, m_ctx));
    } else {
        /*remote*/
        delete_task.reset(new RemoteDeleteTask(backup_name, m_ctx));
    }
    m_task_schedule_list.push_back(delete_task);
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::restore_backup(const RestoreBackupInReq* req, ServerWriter<RestoreBackupInAck>* writer) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupType  restore_backup_type = req->backup_type();
    LOG_INFO << "restore backup vname:" << vol_name << " bname:" << backup_name << " btype:" << restore_backup_type;
    if (!m_ctx->is_backup_exist(backup_name)) {
        RestoreBackupInAck ack;
        ack.set_status(StatusCode::sBackupNotExist);
        ack.set_blk_over(true);
        writer->Write(ack);
        LOG_INFO << "restore backup vname:" << vol_name
                << " bname:" << backup_name << " btype:" << restore_backup_type
                << " failed not exist";
        return StatusCode::sOk;
    }
    m_ctx->update_backup_status(backup_name, BackupStatus::BACKUP_RESTORING);
    if (restore_backup_type == BackupType::BACKUP_LOCAL) {
        ret = local_restore(backup_name, writer);
    } else {
        ret = remote_restore(backup_name, writer);
    }
    m_ctx->update_backup_status(backup_name, BackupStatus::BACKUP_AVAILABLE);
    LOG_INFO << "restore backup vname:" << vol_name
             << " bname:" << backup_name << " btype:" << restore_backup_type << " ok";
    return ret;
}

StatusCode BackupMds::local_restore(const std::string& bname, ServerWriter<RestoreBackupInAck>* writer) {
    std::string backup_base = m_ctx->get_backup_base(bname);
    std::string cur_backup = backup_base;
    LOG_INFO << "local restore bname:" << bname << " begin";
    while (!cur_backup.empty()) {
        backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
        backup_block_t pivot;
        pivot.vol_name = m_ctx->vol_name();
        pivot.backup_id = cur_backup_id;
        IndexStore::IndexIterator it = m_ctx->index_store()->db_iterator();
        it->seek_to_first(pivot.prefix());
        for (; it->valid()&&(-1 != it->key().find(pivot.prefix())); it->next()) {
            backup_block_t cur_block;
            cur_block.decode(it->value());
            RestoreBackupInAck ack;
            ack.set_blk_no(cur_block.block_no);
            ack.set_blk_zero(cur_block.block_zero);
            ack.set_blk_url(cur_block.block_url);
            writer->Write(ack);
        }
        if (cur_backup.compare(bname) == 0) {
            /*arrive the end backup*/
            break;
        }
        cur_backup = m_ctx->get_next_backup(cur_backup);
    }
    LOG_INFO << "local restore bname:" << bname << " end";
    return StatusCode::sOk;
}

StatusCode BackupMds::remote_restore(const std::string& bname, ServerWriter<RestoreBackupInAck>* writer) {
    ClientContext rpc_ctx;
    grpc_stream_ptr remote_stream = NetSender::instance().create_stream(&rpc_ctx);
    DownloadDataReq download_req;
    download_req.set_vol_name(m_ctx->vol_name());
    download_req.set_backup_name(bname);
    std::string download_req_buf;
    download_req.SerializeToString(&download_req_buf);
    TransferRequest transfer_req;
    transfer_req.set_type(MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);
    transfer_req.set_data(download_req_buf.c_str(), download_req_buf.length());
    LOG_INFO << "remote restore bname:" << bname << " begin";
    if (!remote_stream->Write(transfer_req)) {
        LOG_ERROR << "send remote download req failed";
        return StatusCode::sInternalError;
    }
    TransferResponse transfer_res;
    while (remote_stream->Read(&transfer_res)) {
        assert(transfer_res.type() == MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);
        DownloadDataAck download_ack;
        download_ack.ParseFromArray(transfer_res.data().c_str(), transfer_res.data().length());
        RestoreBackupInAck restore_ack;
        if (download_ack.blk_over()) {
            restore_ack.set_blk_over(true);
            bool bwrite = writer->Write(restore_ack);
            assert(bwrite);
            break;
        }
        restore_ack.set_blk_no(download_ack.blk_no());
        restore_ack.set_blk_zero(download_ack.blk_zero());
        restore_ack.set_blk_url(download_ack.blk_url());
        restore_ack.set_blk_data(download_ack.blk_data().c_str(), download_ack.blk_data().length());
        bool bwrite = writer->Write(restore_ack);
        assert(bwrite);
    }
    LOG_INFO << "remote restore bname:" << bname << " end";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_create_start(const RemoteBackupStartReq* req, RemoteBackupStartAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_mode();
    BackupType backup_type = req->backup_type();
    LOG_INFO << "do remote create start vname:" << vol_name << " bname:" << backup_name;
    ret = prepare_create(backup_name, backup_mode, backup_type, true);
    if (ret) {
        ack->set_status(ret);
        LOG_INFO << "do remote create start vname:" << vol_name << " bname:" << backup_name <<" failed";
        return ret;
    }
    LOG_INFO << "do remote create start vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_create_upload(UploadDataReq* req, UploadDataAck* ack) {
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    block_t blk_no = req->blk_no();
    off_t blk_off = req->blk_off();
    bool blk_zero = req->blk_zero();
    const char* blk_data = req->blk_data().c_str();
    size_t blk_len = req->blk_data().length();
    LOG_INFO << "do remote upload vname:" << vol_name << " bname:" << backup_name;
    if (!m_ctx->is_backup_exist(backup_name)) {
        LOG_ERROR << "do remote upload vname:" << vol_name << " bname:" << backup_name << " failed not exist";
        return StatusCode::sBackupNotExist;
    }
    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    backup_block_t cur_block;
    cur_block.vol_name = vol_name;
    cur_block.backup_id = m_ctx->get_backup_id(backup_name);
    cur_block.block_no = blk_no;
    cur_block.block_zero = blk_zero;
    cur_block.block_url = spawn_backup_block_url(cur_block);
    if (!blk_zero) {
        /*store backup data to block store in object*/
        int write_ret = m_ctx->block_store()->write(cur_block.block_url,
                                                    const_cast<char*>(blk_data),
                                                    blk_len, blk_off);
        if (write_ret != 0) {
            LOG_ERROR << "do remote update wrie url:" << cur_block.block_url; 
            return StatusCode::sInternalError;
        }
    }
    m_ctx->index_store()->db_put(cur_block.key(), cur_block.encode());
    LOG_INFO << "do remote upload vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_create_end(const RemoteBackupEndReq* req, RemoteBackupEndAck* ack) {
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    LOG_INFO << "do remote create end vname:" << vol_name << " bname:" << backup_name;
    if (!m_ctx->is_backup_exist(backup_name)) {
        LOG_ERROR << "do remote create end vname:" << vol_name
                  << " bname:" << backup_name << " failed not exist";
        return StatusCode::sBackupNotExist;
    }
    m_ctx->update_backup_status(backup_name, BackupStatus::BACKUP_AVAILABLE);
    m_ctx->trace();
    LOG_INFO << " do remote create end vname:" << vol_name
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_delete(const RemoteBackupDeleteReq* req, RemoteBackupDeleteAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    LOG_INFO << "do remote delete vname:" << vol_name << " bname:" << backup_name;
    ret = prepare_delete(backup_name);
    if (ret) {
        ack->set_status(ret);
        LOG_ERROR << "do remote delete vname:" << vol_name << " bname:" << backup_name << " failed";
        return ret;
    }
    /*async task*/
    std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
    shared_ptr<AsyncTask> delete_task(new LocalDeleteTask(backup_name, m_ctx));
    m_task_schedule_list.push_back(delete_task);
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "do remote delete vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_download(const DownloadDataReq* req, ServerReaderWriter<TransferResponse, TransferRequest>* stream) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    LOG_INFO << "do remote download vname:" << vol_name << " bname:" << backup_name;
    if (!m_ctx->is_backup_exist(backup_name)) {
        LOG_ERROR << "do remote download vname:" << vol_name << " bname:" << backup_name << " failed not exist";
        /*notify no any more data any more*/
        DownloadDataAck end_ack;
        end_ack.set_blk_over(true);
        std::string end_ack_buf;
        end_ack.SerializeToString(&end_ack_buf);
        TransferResponse res;
        res.set_type(MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);
        res.set_data(end_ack_buf.c_str(), end_ack_buf.length());
        bool bret = stream->Write(res);
        assert(bret == true);
        return StatusCode::sBackupNotExist;
    }
    std::string first_backup = m_ctx->get_backup_base(backup_name);
    std::string last_backup = backup_name;
    std::string cur_backup = first_backup; 
    char* buf = new char[BACKUP_BLOCK_SIZE];
    assert(buf != nullptr);
    while (true) {
        IndexStore::IndexIterator it = m_ctx->index_store()->db_iterator();
        backupid_t backup_id = m_ctx->get_backup_id(cur_backup);
        backup_block_t pivot;
        pivot.vol_name = vol_name;
        pivot.backup_id = backup_id;
        it->seek_to_first(pivot.prefix());
        for (; it->valid()&&(-1 != it->key().find(pivot.prefix())); it->next()) {
            backup_block_t cur_block;
            cur_block.decode(it->value());
            uint64_t blk_no = cur_block.block_no;
            bool blk_zero = cur_block.block_zero;
            std::string blk_url = cur_block.block_url;
            DownloadDataAck ack;
            ack.set_blk_no(blk_no);
            ack.set_blk_url(blk_url);
            ack.set_blk_zero(blk_zero); 
            LOG_INFO << "download data blk_no:" << blk_no << " blk_zero:" << blk_zero << " blk_url:" << blk_url;
            if (!blk_zero) {
                int read_ret = m_ctx->block_store()->read(blk_url, buf, BACKUP_BLOCK_SIZE, 0);
                assert(read_ret == BACKUP_BLOCK_SIZE);
                ack.set_blk_data(buf, BACKUP_BLOCK_SIZE);
            }
            std::string ack_buf;
            ack.SerializeToString(&ack_buf);
            TransferResponse res;
            res.set_type(MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);
            res.set_data(ack_buf.c_str(), ack_buf.length());
            bool bret = stream->Write(res);
            assert(bret == true);
        }
        if (0 == cur_backup.compare(last_backup)) {
            LOG_INFO << "download last backup:" << cur_backup;
            break;
        }
        LOG_INFO << "download backup current:" << cur_backup;
        cur_backup = m_ctx->get_next_backup(cur_backup);
        LOG_INFO << "download backup next:" << cur_backup;
    }
    if (buf) {
        delete [] buf;
    }
    /*notify no any more data any more*/
    DownloadDataAck end_ack;
    end_ack.set_blk_over(true);
    std::string end_ack_buf;
    end_ack.SerializeToString(&end_ack_buf);
    TransferResponse res;
    res.set_type(MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);
    res.set_data(end_ack_buf.c_str(), end_ack_buf.length());
    bool bret = stream->Write(res);
    assert(bret == true);
    LOG_INFO << "do remote download vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

int BackupMds::recover() {

    LOG_INFO << "backup meta data recover";
    IndexStore::IndexIterator it = m_ctx->index_store()->db_iterator();
    it->seek_to_last(backup_prefix);
    if (!it->valid()) {
        LOG_ERROR << "recover no backup exist now";
        m_ctx->set_latest_backup_id(BACKUP_INIT_UUID);
        return 0;
    }
    backup_t current;
    current.decode(it->value());
    m_ctx->set_latest_backup_id(current.backup_id); 
    it->seek_to_first(backup_prefix);
    for (; it->valid()&&(-1 != it->key().find(backup_prefix)); it->next()) {
        backup_t current;
        current.decode(it->value());
        if (current.backup_status == BackupStatus::BACKUP_CREATING) {
            std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
            shared_ptr<AsyncTask> create_task;
            if (current.backup_type == BackupType::BACKUP_LOCAL) {
                create_task.reset(new LocalCreateTask(current.backup_name, m_ctx));
            } else {
                create_task.reset(new RemoteCreateTask(current.backup_name, m_ctx));
            }
            m_task_schedule_list.push_back(create_task);
            continue;
        }

        if (current.backup_status == BackupStatus::BACKUP_DELETING) {
            std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
            shared_ptr<AsyncTask> delete_task;
            if (current.backup_type == BackupType::BACKUP_LOCAL) {
                delete_task.reset(new LocalDeleteTask(current.backup_name, m_ctx));
            } else {
                delete_task.reset(new RemoteDeleteTask(current.backup_name, m_ctx));
            }
            m_task_schedule_list.push_back(delete_task);
            continue;
        }
    }
    m_ctx->trace();
    LOG_INFO << "backup meta data recover ok";
    return StatusCode::sOk;
}

int BackupMds::do_task_schedule() {
    list<shared_ptr<AsyncTask>> m_task_run_list;
    while (m_task_schedule_run) {
        if (m_task_schedule_list.empty() && m_task_run_list.empty()) {
            usleep(200);
            continue;
        }
        {
            std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
            for (auto it = m_task_schedule_list.begin(); it != m_task_schedule_list.end();) {
                if ((*it)->ready()) {
                    LOG_INFO << "task:" << (*it)->task_name() << " will run";
                    m_task_run_list.push_back(*it);
                    m_thread_pool->submit(bind(&AsyncTask::work, (*it).get()));
                    it = m_task_schedule_list.erase(it);
                } else {
                    it++;
                }
            }
        }
        for (auto it = m_task_run_list.begin(); it != m_task_run_list.end();) {
            if ((*it)->finish()) {
                LOG_INFO << "task:" << (*it)->task_name() << " will remove";
                it = m_task_run_list.erase(it);
            } else {
                it++;
            }
        }
    }
}
