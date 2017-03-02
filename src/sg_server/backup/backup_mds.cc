#include <cstdlib>
#include <assert.h>
#include "log/log.h"
#include "common/define.h"
#include "common/utils.h"
#include "backup_util.h"
#include "backup_ctx.h"
#include "backup_task.h"
#include "backup_mds.h"

using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;

BackupMds::BackupMds(const string& vol_name, const size_t& vol_size)
{
    m_ctx.reset(new BackupCtx(vol_name, vol_size));
    m_task_tracer_run.store(true);
    m_thread_pool.reset(new ThreadPool(5));
    m_thread_pool->submit(bind(&BackupMds::trace_task, this));
}

BackupMds::~BackupMds()
{
    m_ctx.reset();
    m_task_tracer_list.clear();
    m_task_tracer_run.store(false);
    m_thread_pool.reset();
}

StatusCode BackupMds::prepare_create(const string& bname, const BackupMode& bmode, 
                                     const BackupType& btype)
{
    /*check whether backup exist*/
    if(m_ctx->is_backup_exist(bname)){
        LOG_INFO << "create backup vname:" << m_ctx->vol_name() <<" bname:" 
                 << bname <<"failed already exist";
        return StatusCode::sBackupAlreadyExist; 
    }
    
    /*check whether incr backup allowable*/
    if(bmode == BackupMode::BACKUP_INCR){
        if(!m_ctx->is_incr_backup_allowable()){
            LOG_INFO << "create backup vname:" << m_ctx->vol_name() <<" bname:" 
                     << bname <<" failed incr has no full backup exist";
            return StatusCode::sBackupNoFullbackupExist;
        } 
    }
    
    /*create backup*/
    backup_attr_t cur_backup_attr;
    cur_backup_attr.volume_uuid = m_ctx->vol_name();
    cur_backup_attr.backup_mode = bmode;
    cur_backup_attr.backup_name = bname;
    /*backup_id generated when backup is created*/
    cur_backup_attr.backup_status = BackupStatus::BACKUP_CREATING;

    /*in memroy update backup status*/
    m_ctx->cur_backups_map().insert({bname, cur_backup_attr});
    /*record current active backup*/
    m_ctx->set_cur_backup_name(bname);
    
    /*db persist */
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey = spawn_latest_backup_name_key();
    transaction->put(pkey, bname);
    pkey = spawn_backup_attr_map_key(bname);
    string pval = spawn_backup_attr_map_val(cur_backup_attr);
    transaction->put(pkey, pval);
    m_ctx->index_store()->submit_transaction(transaction);
 
    return StatusCode::sOk;
}

StatusCode BackupMds::prepare_delete(const string& bname)
{
    /*check whether backup exist*/
    if(!m_ctx->is_backup_exist(bname)){
        LOG_INFO << "delete backup vname:" << m_ctx->vol_name() <<" bname:" 
                 << bname <<"failed not exist";
        return StatusCode::sBackupNotExist; 
    }

    /*check whether backup can be delete*/
    if(!m_ctx->is_backup_deletable(bname)){
        LOG_INFO << "delete backup vname:" << m_ctx->vol_name() <<" bname:" 
                 << bname <<" failed can not delete";
        return StatusCode::sBackupCanNotDelete; 
    }

    /*delete backup*/
    {
        auto it = m_ctx->cur_backups_map().find(bname);
        assert(it != m_ctx->cur_backups_map().end());
        /*in memroy update backup status*/
        it->second.backup_status = BackupStatus::BACKUP_DELETING; 
        /*(todo)in db update snapshot status*/
    }
    
    return StatusCode::sOk;
}

StatusCode BackupMds::create_backup(const CreateBackupInReq* req, CreateBackupInAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name    = req->vol_name();
    string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_option().backup_mode();
    BackupType backup_type = req->backup_option().backup_type();

    LOG_INFO << "create backup vname:" << m_ctx->vol_name() << " bname:" << backup_name;
    
    ret = prepare_create(backup_name, backup_mode, backup_type);
    if(ret){
        ack->set_status(ret);
        LOG_INFO << "create backup vname:" << m_ctx->vol_name() <<" bname:" << backup_name <<" failed";
        return ret;
    }

    /*async task*/
    std::lock_guard<std::mutex> scope_guard(m_task_tracer_lock);
    shared_ptr<BackupTask> create_task(new CreateBackupTask(backup_name, m_ctx));
    m_task_tracer_list.push_back(create_task);
    m_thread_pool->submit(bind(&BackupTask::work, create_task.get()));

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "create backup vname:" << m_ctx->vol_name() <<" bname:" << backup_name <<" ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::list_backup(const ListBackupInReq* req, ListBackupInAck* ack)
{
    string vol_name = req->vol_name();
    LOG_INFO << "list backup vname:" << m_ctx->vol_name();
   
    /*return backup names*/
    for(auto it : m_ctx->cur_backups_map()){
        ack->add_backup_name(it.first);
    }
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "list backup vname:" << m_ctx->vol_name() << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::get_backup(const GetBackupInReq* req, GetBackupInAck* ack)
{
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "query backup vname:" << m_ctx->vol_name() << " bname:" << backup_name;
    BackupStatus backup_status = m_ctx->get_backup_status(backup_name);
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "query backup vname:" << m_ctx->vol_name() << " bname:" << backup_name 
             << " bstatus:" << backup_status << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::delete_backup(const DeleteBackupInReq* req, DeleteBackupInAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete backup vname:" << m_ctx->vol_name() << " bname:" << backup_name;
    
    ret = prepare_delete(backup_name);
    if(ret){
        ack->set_status(ret);
        LOG_INFO << "delete backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " ok";
        return ret; 
    } 

    /*async task*/
    std::lock_guard<std::mutex> scope_guard(m_task_tracer_lock);
    shared_ptr<BackupTask> delete_task(new DeleteBackupTask(backup_name, m_ctx));
    m_task_tracer_list.push_back(delete_task);
    m_thread_pool->submit(bind(&BackupTask::work, delete_task.get()));

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "delete backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::restore_backup(const RestoreBackupInReq* req,  
                                     ServerWriter<RestoreBackupInAck>* writer)
{
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "restore backup vname:" << m_ctx->vol_name() << " bname:" << backup_name;
    string backup_base = m_ctx->get_backup_base(backup_name);
    string cur_backup  = backup_base;

    while(!cur_backup.empty())
    {
        backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
        auto cur_backup_block_map_it = m_ctx->cur_blocks_map().find(cur_backup_id);
        assert(cur_backup_block_map_it != m_ctx->cur_blocks_map().end());
        auto cur_backup_block_map = cur_backup_block_map_it->second;

        for(auto cur_block : cur_backup_block_map){
            RestoreBackupInAck ack;
            ack.set_blk_no(cur_block.first);
            ack.set_blk_obj(cur_block.second);
            writer->Write(ack);
        }

        if(cur_backup.compare(backup_name) == 0){
            /*arrive the end backup*/
            break;
        }
    }

    LOG_INFO << "restore backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

/*call by remote backup*/

StatusCode BackupMds::create_remote(const CreateRemoteBackupInReq* req, 
                                    CreateRemoteBackupInAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name    = req->vol_name();
    string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_option().backup_mode();
    BackupType backup_type = req->backup_option().backup_type();

    LOG_INFO << "create remote backup vname:" << m_ctx->vol_name() 
             << " bname:" << backup_name;

    ret = prepare_create(backup_name, backup_mode, backup_type);
    if(ret){
        ack->set_status(ret);
        LOG_INFO << "create remote backup vname:" << m_ctx->vol_name() 
                 << " bname:" << backup_name <<" failed";
        return ret;
    }

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "create remote backup vname:" << m_ctx->vol_name() 
             <<" bname:" << backup_name <<" ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::delete_remote(const DeleteRemoteBackupInReq* req, 
                                    DeleteRemoteBackupInAck* ack)
{
    StatusCode ret = StatusCode::sOk;
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete remote backup vname:" << m_ctx->vol_name() << " bname:" << backup_name;
    
    ret = prepare_delete(backup_name);
    if(ret){
        ack->set_status(ret);
        LOG_INFO << "delete remote backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " ok";
        return ret; 
    } 

    /*async task*/
    std::lock_guard<std::mutex> scope_guard(m_task_tracer_lock);
    shared_ptr<BackupTask> delete_task(new DeleteBackupTask(backup_name, m_ctx));
    m_task_tracer_list.push_back(delete_task);
    m_thread_pool->submit(bind(&BackupTask::work, delete_task.get()));

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "delete remote backup vname:" << m_ctx->vol_name() << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::upload_start(const string& backup_name)
{
    auto it = m_ctx->cur_backups_map().find(backup_name);
    if(it == m_ctx->cur_backups_map().end()){
        LOG_INFO << "create backup vname:" << m_ctx->vol_name() <<" bname:" 
                 << backup_name <<"failed already exist";
        return StatusCode::sBackupNotExist; 
    }

    /*generate backup id*/
    backupid_t backup_id  = m_ctx->spawn_backup_id();

    /*update backup attr*/
    it->second.backup_id     = backup_id;

    /*prepare backup block map*/
    map<block_t, backup_object_t> block_map;
    m_ctx->cur_blocks_map().insert({backup_id, block_map});

    return StatusCode::sOk;
}

StatusCode BackupMds::upload(const string& backup_name, const block_t& blk_no, 
                             const char* blk_data, const size_t& blk_len)
{
    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    backup_object_t blk_obj = spawn_backup_object_name(m_ctx->vol_name(), 
                                  backup_id, blk_no);

    /*store backup data to block store in object*/
    int write_ret = m_ctx->block_store()->write(blk_obj, (char*)blk_data, blk_len, 0);
    assert(write_ret == 0);

    /*update backup block map*/
    auto backup_block_map_it = m_ctx->cur_blocks_map().find(backup_id);
    assert(backup_block_map_it != m_ctx->cur_blocks_map().end());
    map<block_t, backup_object_t> &backup_block_map = backup_block_map_it->second;
    backup_block_map.insert({blk_no, blk_obj});

    return StatusCode::sOk;
}

StatusCode BackupMds::upload_over(const string& backup_name)
{
    auto it = m_ctx->cur_backups_map().find(backup_name);
    if(it == m_ctx->cur_backups_map().end()){
        LOG_INFO << "create backup vname:" << m_ctx->vol_name() <<" bname:" 
            << backup_name <<"failed already exist";
        return StatusCode::sBackupNotExist; 
    }
    it->second.backup_status = BackupStatus::BACKUP_CREATED;

    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey = spawn_latest_backup_id_key();
    string pval = to_string(m_ctx->latest_backup_id());
    transaction->put(pkey, pval);
    pkey = spawn_backup_attr_map_key(backup_name);
    pval = spawn_backup_attr_map_val(it->second);
    transaction->put(pkey, pval);
    
    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    auto block_map_it = m_ctx->cur_blocks_map().find(backup_id);
    auto& block_map = block_map_it->second;
    for(auto block : block_map){
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        pval = block.second;
        transaction->put(pkey, pval);
    }
    m_ctx->index_store()->submit_transaction(transaction);
    return StatusCode::sOk;
}

StatusCode BackupMds::download(const DownloadReq* req, ServerWriter<DownloadAck>* writer)
{
    string backup_name = req->backup_name();

    backupid_t backup_id = m_ctx->get_backup_id(backup_name);
    auto block_map_it = m_ctx->cur_blocks_map().find(backup_id);
    auto& block_map = block_map_it->second;
    
    char* buf = (char*)malloc(BACKUP_BLOCK_SIZE);
    assert(buf != nullptr);

    for(auto block : block_map)
    {
        uint64_t blk_no = block.first;
        string   blk_obj = block.second;
        
        int read_ret = m_ctx->block_store()->read(blk_obj, buf, BACKUP_BLOCK_SIZE, 0);
        assert(read_ret == BACKUP_BLOCK_SIZE);
        
        DownloadAck ack;
        ack.set_blk_no(blk_no);
        ack.set_blk_data(buf, BACKUP_BLOCK_SIZE);
        writer->Write(ack);
    }
    
    if(buf){
        free(buf);
    }
    return StatusCode::sOk;
}

int BackupMds::recover()
{
    IndexStore::SimpleIteratorPtr it = m_ctx->index_store()->db_iterator();
    
    /*recover latest backup id*/
    string prefix = BACKUP_ID_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
          it->next()){
        string value = it->value();
        m_ctx->set_latest_backup_id(atol(value.c_str()));
    } 
    
    /*recover latest backup name*/
    prefix = BACKUP_NAME_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
          it->next()){
        string value = it->value();
        m_ctx->set_cur_backup_name(value);
    } 
    
    /*recover backup attr map*/
    prefix = BACKUP_MAP_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
            it->next()){
        string key = it->key();
        string value = it->value();
        
        string backup_name;
        split_backup_attr_map_key(key, backup_name);

        backup_attr_t backup_attr;
        split_backup_attr_map_val(value, backup_attr);

        m_ctx->cur_backups_map().insert({backup_name, backup_attr});
    } 
  
    /*recover backup block map*/
    for(auto backup : m_ctx->cur_backups_map()){
        backupid_t backup_id = backup.second.backup_id;
        map<block_t, backup_object_t> block_map;
        
        prefix = BACKUP_BLOCK_PREFIX;
        prefix.append(BACKUP_FS);
        prefix.append(to_string(backup_id));
        prefix.append(BACKUP_FS);
        it->seek_to_first(prefix);                                                  

        for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
                it->next()){
            backupid_t backup_id;
            block_t    blk_id;
            split_backup_block_map_key(it->key(), backup_id, blk_id);
            block_map.insert({blk_id, it->value()});
        } 
        
        m_ctx->cur_blocks_map().insert({backup_id, block_map});
    }
    
    m_ctx->trace();
    LOG_INFO << "drserver recover backup meta data ok";
    return 0;
}

int BackupMds::trace_task()
{
    while(m_task_tracer_run)
    {
        if(m_task_tracer_list.empty()){
            usleep(200);
            continue;
        }

        for(auto task : m_task_tracer_list){
            /*todo task redo or others*/
        }
    }
}
