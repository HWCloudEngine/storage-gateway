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
#include "backup_util.h"
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

StatusCode BackupMds::prepare_create(const std::string& bname,
        const BackupMode& bmode, const BackupType& btype, bool on_remote) {
    LOG_INFO << "prepare create vname:" << m_ctx->vol_name()
             << " bname:" << bname << " on_remote:" << on_remote;
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
    backup_attr_t cur_backup_attr;
    cur_backup_attr.volume_uuid = m_ctx->vol_name();
    cur_backup_attr.backup_mode = bmode;
    cur_backup_attr.backup_name = bname;
    cur_backup_attr.backup_type = btype;
    /*backup_id generated when backup is created*/
    cur_backup_attr.backup_status = BackupStatus::BACKUP_CREATING;

    /*in memroy update backup status*/
    m_ctx->cur_backups_map().insert({bname, cur_backup_attr});
    /*db persist */
    std::string pkey = spawn_backup_attr_map_key(bname);
    std::string pval = spawn_backup_attr_map_val(cur_backup_attr);
    m_ctx->index_store()->db_put(pkey, pval);

    LOG_INFO << "prepare create vname:" << m_ctx->vol_name()
             << " bname:" << bname << " on_remote:" << on_remote << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::prepare_delete(const std::string& bname) {
    LOG_INFO << "prepare delete vname:" << m_ctx->vol_name()
             << " bname:" << bname;

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
    {
        auto it = m_ctx->cur_backups_map().find(bname);
        assert(it != m_ctx->cur_backups_map().end());
        /*in memroy update backup status*/
        it->second.backup_status = BackupStatus::BACKUP_DELETING;
        /*in db update snapshot status*/
        std::string pkey = spawn_backup_attr_map_key(bname);
        std::string pval = spawn_backup_attr_map_val(it->second);
        m_ctx->index_store()->db_put(pkey, pval);
    }

    LOG_INFO << "prepare delete vname:" << m_ctx->vol_name()
             << " bname:" << bname << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::create_backup(const CreateBackupInReq* req,
                                    CreateBackupInAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_option().backup_mode();
    BackupType backup_type = req->backup_option().backup_type();

    LOG_INFO << "create backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " btype:" << backup_type;

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
    LOG_INFO << "create backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " btype:" << backup_type << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::list_backup(const ListBackupInReq* req,
                                  ListBackupInAck* ack) {
    std::string vol_name = req->vol_name();
    LOG_INFO << "list backup vname:" << m_ctx->vol_name();

    /*return backup names*/
    for (auto it : m_ctx->cur_backups_map()) {
        ack->add_backup_name(it.first);
    }
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "list backup vname:" << m_ctx->vol_name() << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::get_backup(const GetBackupInReq* req,
                                 GetBackupInAck* ack) {
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    LOG_INFO << "Get backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;
    if (!m_ctx->is_backup_exist(backup_name)) {
        ack->set_status(StatusCode::sOk);
        ack->set_backup_status(BackupStatus::BACKUP_DELETED);
        LOG_INFO << "Get backup vname:" << m_ctx->vol_name()
                 << " bname:" << backup_name << "failed not exist";
        return StatusCode::sOk;
    }
    BackupStatus backup_status = m_ctx->get_backup_status(backup_name);
    ack->set_status(StatusCode::sOk);
    ack->set_backup_status(backup_status);
    LOG_INFO << "Get backup vname:" << m_ctx->vol_name()
        << " bname:" << backup_name << " bstatus:" << backup_status << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::delete_backup(const DeleteBackupInReq* req,
                                    DeleteBackupInAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupType backup_type = m_ctx->get_backup_type(backup_name);

    LOG_INFO << "delete backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;

    ret = prepare_delete(backup_name);
    if (ret) {
        ack->set_status(ret);
        LOG_ERROR << " prepare delete failed";
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
    LOG_INFO << "delete backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::restore_backup(const RestoreBackupInReq* req,
                            ServerWriter<RestoreBackupInAck>* writer) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupType backup_type = m_ctx->get_backup_type(backup_name);
    LOG_INFO << "restore backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;
    m_ctx->update_backup_status(backup_name, BackupStatus::BACKUP_RESTORING);
    if (backup_type == BackupType::BACKUP_LOCAL) {
        ret = local_restore(backup_name, writer);
    } else {
        ret = remote_restore(backup_name, writer);
    }
    m_ctx->update_backup_status(backup_name, BackupStatus::BACKUP_AVAILABLE);
    LOG_INFO << "restore backup vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return ret;
}

StatusCode BackupMds::local_restore(const std::string& bname,
                        ServerWriter<RestoreBackupInAck>* writer) {
    std::string backup_base = m_ctx->get_backup_base(bname);
    std::string cur_backup  = backup_base;
    LOG_INFO << " local restore bname:" << bname << " begin";
    while (!cur_backup.empty()) {
        backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
        auto cur_backup_block_map_it = m_ctx->cur_blocks_map().find(cur_backup_id);
        assert(cur_backup_block_map_it != m_ctx->cur_blocks_map().end());
        auto cur_backup_block_map = cur_backup_block_map_it->second;

        for (auto cur_block : cur_backup_block_map) {
            RestoreBackupInAck ack;
            ack.set_blk_no(cur_block.first);
            ack.set_blk_obj(cur_block.second);
            writer->Write(ack);
        }

        if (cur_backup.compare(bname) == 0) {
            /*arrive the end backup*/
            break;
        }
        cur_backup = m_ctx->get_next_backup(cur_backup);
    }
    LOG_INFO << " local restore bname:" << bname << " end";
    return StatusCode::sOk;
}

StatusCode BackupMds::remote_restore(const std::string& bname,
                 ServerWriter<RestoreBackupInAck>* writer) {
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
    LOG_INFO << " remote restore bname:" << bname << " begin";

    if (!remote_stream->Write(transfer_req)) {
        LOG_ERROR << "send remote download req failed";
        return StatusCode::sInternalError;
    }

    TransferResponse transfer_res;
    while (remote_stream->Read(&transfer_res)) {
        assert(transfer_res.type() == MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);

        DownloadDataAck download_ack;
        download_ack.ParseFromArray(transfer_res.data().c_str(), \
                                    transfer_res.data().length());

        RestoreBackupInAck restore_ack;
        if (download_ack.blk_over()) {
            restore_ack.set_blk_over(true);
            bool bwrite = writer->Write(restore_ack);
            assert(bwrite);
            break;
        }

        restore_ack.set_blk_no(download_ack.blk_no());
        restore_ack.set_blk_data(download_ack.blk_data().c_str(), \
                                 download_ack.blk_data().length());
        bool bwrite = writer->Write(restore_ack);
        assert(bwrite);
    }

    LOG_INFO << " remote restore bname:" << bname << " end";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_create_start(const RemoteBackupStartReq* req,
                                             RemoteBackupStartAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_mode();
    BackupType backup_type = req->backup_type();

    LOG_INFO << " do remote create start vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;

    ret = prepare_create(backup_name, backup_mode, backup_type, true);
    if (ret) {
        ack->set_status(ret);
        LOG_INFO << "do remote create start vname:" << m_ctx->vol_name()
                 << " bname:" << backup_name <<" failed";
        return ret;
    }
    auto it = m_ctx->cur_backups_map().find(backup_name);
    if (it == m_ctx->cur_backups_map().end()) {
        LOG_INFO << "do remote create start vname:" << m_ctx->vol_name()
                 << " bname:" << backup_name <<"failed already exist";
        return StatusCode::sBackupNotExist;
    }

    /*generate backup id*/
    backupid_t backup_id  = m_ctx->spawn_backup_id();

    /*update backup attr*/
    it->second.backup_id     = backup_id;

    /*prepare backup block map*/
    map<block_t, backup_object_t> block_map;
    m_ctx->cur_blocks_map().insert({backup_id, block_map});

    LOG_INFO << " do remote create start vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_create_upload(UploadDataReq* req,
                                              UploadDataAck* ack) {
    std::string backup_name = req->backup_name();
    block_t blk_no = req->blk_no();
    off_t blk_off = req->blk_off();
    const char* blk_data = req->blk_data().c_str();
    size_t blk_len = req->blk_data().length();

    LOG_INFO << " do remote upload vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;

    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    backup_object_t blk_obj = spawn_backup_object_name(m_ctx->vol_name(), \
                                                       backup_id, blk_no);

    /*store backup data to block store in object*/
    int write_ret = m_ctx->block_store()->write(blk_obj,
                                                const_cast<char*>(blk_data),
                                                blk_len, blk_off);
    assert(write_ret == 0);

    /*update backup block map*/
    auto backup_block_map_it = m_ctx->cur_blocks_map().find(backup_id);
    assert(backup_block_map_it != m_ctx->cur_blocks_map().end());
    map<block_t, backup_object_t> &backup_block_map = backup_block_map_it->second;
    backup_block_map.insert({blk_no, blk_obj});

    LOG_INFO << " do remote upload vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_create_end(const RemoteBackupEndReq* req,
                                           RemoteBackupEndAck* ack) {
    std::string backup_name = req->backup_name();

    LOG_INFO << " do remote create end vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;

    auto it = m_ctx->cur_backups_map().find(backup_name);
    if (it == m_ctx->cur_backups_map().end()) {
        LOG_ERROR << "do remote create end vname:" << m_ctx->vol_name()
                  << " bname:" << backup_name <<"failed already exist";
        return StatusCode::sBackupNotExist;
    }
    it->second.backup_status = BackupStatus::BACKUP_AVAILABLE;

    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    std::string pkey = spawn_latest_backup_id_key();
    std::string pval = std::to_string(m_ctx->latest_backup_id());
    transaction->put(pkey, pval);
    pkey = spawn_backup_attr_map_key(backup_name);
    pval = spawn_backup_attr_map_val(it->second);
    transaction->put(pkey, pval);

    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    auto block_map_it = m_ctx->cur_blocks_map().find(backup_id);
    auto& block_map = block_map_it->second;
    for (auto block : block_map) {
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        pval = block.second;
        transaction->put(pkey, pval);
    }
    m_ctx->index_store()->submit_transaction(transaction);

    m_ctx->trace();
    LOG_INFO << " do remote create end vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_delete(const RemoteBackupDeleteReq* req,
                                       RemoteBackupDeleteAck* ack) {
    StatusCode ret = StatusCode::sOk;
    std::string vol_name = req->vol_name();
    std::string backup_name = req->backup_name();
    LOG_INFO << "do remote delete vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;
    ret = prepare_delete(backup_name);
    if (ret) {
        ack->set_status(ret);
        LOG_ERROR << "do remote delete vname:" << m_ctx->vol_name()
                  << " bname:" << backup_name << " ok";
        return ret;
    }

    /*async task*/
    std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
    shared_ptr<AsyncTask> delete_task(new LocalDeleteTask(backup_name, m_ctx));
    m_task_schedule_list.push_back(delete_task);

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "do remote delete vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::do_remote_download(const DownloadDataReq* req,
        ServerReaderWriter<TransferResponse, TransferRequest>* stream) {
    std::string backup_name = req->backup_name();

    LOG_INFO << "do remote download vname:" << m_ctx->vol_name()
             << " bname:" << backup_name;

    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    auto block_map_it = m_ctx->cur_blocks_map().find(backup_id);
    auto& block_map = block_map_it->second;
    char* buf = new char[BACKUP_BLOCK_SIZE];
    assert(buf != nullptr);

    for (auto block : block_map) {
        uint64_t blk_no = block.first;
        std::string   blk_obj = block.second;
        LOG_INFO << "download data blk_no:" << blk_no << " blk_obj:" << blk_obj;
        int read_ret = m_ctx->block_store()->read(blk_obj, buf, \
                                                  BACKUP_BLOCK_SIZE, 0);
        assert(read_ret == BACKUP_BLOCK_SIZE);
        DownloadDataAck ack;
        ack.set_blk_no(blk_no);
        ack.set_blk_data(buf, BACKUP_BLOCK_SIZE);

        std::string ack_buf;
        ack.SerializeToString(&ack_buf);

        TransferResponse res;
        res.set_type(MessageType::REMOTE_BACKUP_DOWNLOAD_DATA);
        res.set_data(ack_buf.c_str(), ack_buf.length());
        bool bret = stream->Write(res);
        assert(bret == true);
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

    LOG_INFO << "do remote download vname:" << m_ctx->vol_name()
             << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

int BackupMds::recover() {
    IndexStore::SimpleIteratorPtr it = m_ctx->index_store()->db_iterator();
    /*recover latest backup id*/
    std::string prefix = BACKUP_ID_PREFIX;
    it->seek_to_first(prefix);
    for (; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str());
          it->next()) {
        std::string value = it->value();
        m_ctx->set_latest_backup_id(atol(value.c_str()));
    }
    /*recover backup attr map*/
    prefix = BACKUP_MAP_PREFIX;
    it->seek_to_first(prefix);
    for (; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str());
            it->next()) {
        std::string key = it->key();
        std::string value = it->value();

        std::string backup_name;
        split_backup_attr_map_key(key, backup_name);

        backup_attr_t backup_attr;
        split_backup_attr_map_val(value, backup_attr);

        m_ctx->cur_backups_map().insert({backup_name, backup_attr});
    }

    /*recover backup block map*/
    for (auto backup : m_ctx->cur_backups_map()) {
        backupid_t backup_id = backup.second.backup_id;
        map<block_t, backup_object_t> block_map;

        prefix = BACKUP_BLOCK_PREFIX;
        prefix.append(BACKUP_FS);
        prefix.append(to_string(backup_id));
        prefix.append(BACKUP_FS);
        it->seek_to_first(prefix);

        for (; it->valid() && !it->key().compare(0, prefix.size(), prefix.c_str());
              it->next()) {
            backupid_t backup_id;
            block_t    blk_id;
            split_backup_block_map_key(it->key(), backup_id, blk_id);
            block_map.insert({blk_id, it->value()});
        }

        m_ctx->cur_blocks_map().insert({backup_id, block_map});
    }
    /*todo: it seems only do on local site
     *1. only valid on local site recover
     *2. what to do on remote site recover
     *3. on remote site, only just load backup meta data
     */
    for (auto backup : m_ctx->cur_backups_map()) {
        if (backup.second.backup_status == BackupStatus::BACKUP_CREATING) {
            std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
            shared_ptr<AsyncTask> create_task;
            if (backup.second.backup_type == BackupType::BACKUP_LOCAL) {
                create_task.reset(new LocalCreateTask(backup.first, m_ctx));
            } else {
               create_task.reset(new RemoteCreateTask(backup.first, m_ctx));
            }
            m_task_schedule_list.push_back(create_task);
            continue;
        }

        if (backup.second.backup_status == BackupStatus::BACKUP_DELETING) {
            std::lock_guard<std::mutex> scope_guard(m_task_schedule_lock);
            shared_ptr<AsyncTask> delete_task;
            if (backup.second.backup_type == BackupType::BACKUP_LOCAL) {
                delete_task.reset(new LocalDeleteTask(backup.first, m_ctx));
            } else {
                delete_task.reset(new RemoteDeleteTask(backup.first, m_ctx));
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
            for (auto it = m_task_schedule_list.begin(); \
                      it != m_task_schedule_list.end();) {
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
