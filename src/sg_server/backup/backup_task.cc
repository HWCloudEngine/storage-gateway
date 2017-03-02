#include <functional>
#include "log/log.h"
#include "common/define.h"
#include "common/utils.h"
#include "rpc/common.pb.h"
#include "backup_util.h"
#include "backup_ctx.h"
#include "backup_task.h"

BackupTask::BackupTask(const string& backup_name, shared_ptr<BackupCtx> ctx)
{
    m_backup_name = backup_name;
    m_ctx = ctx;
}

BackupTask::~BackupTask()
{
}

CreateBackupTask::CreateBackupTask(const string& backup_name, 
                                   shared_ptr<BackupCtx> ctx) 
                 : BackupTask(backup_name, ctx)
{
    m_task_name.append(backup_name);
    m_task_name.append("_create_backup");

    m_task_status = TASK_READY;
}

CreateBackupTask::~CreateBackupTask()
{
    
}

StatusCode CreateBackupTask::do_full_backup()
{
    StatusCode ret_code = StatusCode::sOk;
    off_t  cur_pos = 0;
    off_t  end_pos = m_ctx->vol_size();
    size_t chunk_size = BACKUP_BLOCK_SIZE;
    char*  chunk_buf  = (char*)malloc(chunk_size);
    assert(chunk_buf != nullptr);
    
    string cur_backup = m_backup_name;

    while(cur_pos < end_pos)
    {
        chunk_size =((end_pos-cur_pos) > BACKUP_BLOCK_SIZE) ?  \
                      BACKUP_BLOCK_SIZE : (end_pos-cur_pos);
        
        /*read current snapshot*/
        string cur_snap = backup_to_snap_name(cur_backup);
        ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(), cur_snap, 
                                               chunk_buf, chunk_size, cur_pos);
        assert(ret_code == StatusCode::sOk);
        
        /*spawn block object*/
        block_t cur_blk_no = (cur_pos / BACKUP_BLOCK_SIZE);
        backupid_t cur_backup_id = m_ctx->get_backup_id(cur_backup);
        backup_object_t cur_blk_obj = spawn_backup_object_name(m_ctx->vol_name(), 
                                         cur_backup_id, cur_blk_no);
        
        /*store backup data to block store in object*/
        int write_ret = m_ctx->block_store()->write(cur_blk_obj, chunk_buf, chunk_size, 0);
        assert(write_ret == 0);

        /*update backup block map*/
        auto cur_backup_block_map_it = m_ctx->cur_blocks_map().find(cur_backup_id);
        assert(cur_backup_block_map_it != m_ctx->cur_blocks_map().end());
        map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
        cur_backup_block_map.insert({cur_blk_no, cur_blk_obj});
        
        cur_pos += chunk_size;
    }
    
    if(chunk_buf){
        free(chunk_buf); 
    }

    return ret_code;
}

StatusCode CreateBackupTask::do_incr_backup()
{
    StatusCode ret_code = StatusCode::sOk;
    string cur_backup = m_backup_name;
    string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if(prev_backup.empty()){
        LOG_ERROR << "incr backup:" << cur_backup<< "has no previous backup";
        return StatusCode::sBackupNotExist;
    }

    /*1. diff cur and prev snapshot*/
    string pre_snap = backup_to_snap_name(prev_backup); 
    string cur_snap = backup_to_snap_name(cur_backup);
    /*todo modify as stream interface*/
    vector<DiffBlocks> diff_blocks;
    ret_code = m_ctx->snap_client()->DiffSnapshot(m_ctx->vol_name(), 
                                                  pre_snap, cur_snap, 
                                                  diff_blocks); 
    assert(ret_code == StatusCode::sOk);

    /*2. read diff data(current snapshot and diff region)*/
    char* buf = (char*)malloc(COW_BLOCK_SIZE);
    for(auto it : diff_blocks){
        uint64_t diff_block_no_size = it.diff_block_no_size() ;
        for(int i = 0; i < diff_block_no_size; i++){
            uint64_t diff_block_no = it.diff_block_no(i);
            off_t    diff_block_off = diff_block_no * COW_BLOCK_SIZE;
            size_t   diff_block_size = COW_BLOCK_SIZE;

            ret_code = m_ctx->snap_client()->ReadSnapshot(m_ctx->vol_name(), 
                                                          cur_snap, buf, 
                                                          diff_block_size, 
                                                          diff_block_off);
            assert(ret_code == StatusCode::sOk);

            /*3. append backup meta(block, object) and (object, backup_ref)*/
            block_t cur_backup_blk_no = (diff_block_off / BACKUP_BLOCK_SIZE);
            backupid_t cur_backup_id  = m_ctx->get_backup_id(cur_backup);
            backup_object_t cur_backup_blk_obj = spawn_backup_object_name(m_ctx->vol_name(),
                                                cur_backup_id, cur_backup_blk_no);

            /*store backup data to block store in object*/
            size_t write_size = diff_block_size;
            /*snapshot cow block  unit diff from backup block unit*/
            off_t  write_off = (diff_block_off % BACKUP_BLOCK_SIZE);
            int write_ret = m_ctx->block_store()->write(cur_backup_blk_obj, buf, write_size, write_off);
            assert(write_ret == 0);

            /*update backup block map*/
            auto cur_backup_block_map_it = m_ctx->cur_blocks_map().find(cur_backup_id);
            assert(cur_backup_block_map_it != m_ctx->cur_blocks_map().end());
            map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
            cur_backup_block_map.insert({cur_backup_blk_no, cur_backup_blk_obj});
        }
    }
    
    if(buf){
        free(buf);
    }

    return ret_code;
}

void CreateBackupTask::work()
{   
    string cur_backup = m_backup_name;
    auto it = m_ctx->cur_backups_map().find(cur_backup);
    if(it == m_ctx->cur_backups_map().end()){
        LOG_ERROR << "cur_backup:" << cur_backup<< " no exist";
        m_task_status = TASK_ERROR;
        return; 
    }
    
    m_task_status = TASK_RUN;

    /*query snapshot of current backup whether created*/
    string cur_backup_snap = backup_to_snap_name(cur_backup);
    /*todo times limited or timeout*/
    while(true){
        SnapStatus cur_backup_snap_status;
        m_ctx->snap_client()->QuerySnapshot(m_ctx->vol_name(), 
                                            cur_backup_snap, 
                                            cur_backup_snap_status);
        if(cur_backup_snap_status == SnapStatus::SNAP_CREATED){
            break;
        }
        usleep(200);
    }

    /*generate backup id*/
    backupid_t backup_id  = m_ctx->spawn_backup_id();
    /*update backup attr*/
    it->second.backup_id     = backup_id;
    it->second.backup_status = BackupStatus::BACKUP_CREATED;
    /*prepare backup block map*/
    map<block_t, backup_object_t> block_map;
    m_ctx->cur_blocks_map().insert({backup_id, block_map});
    
    /*do backup and update backup block map*/
    if(it->second.backup_mode == BackupMode::BACKUP_FULL){
        do_full_backup();
    } else if(it->second.backup_mode == BackupMode::BACKUP_INCR){
        do_incr_backup();
    } else {
        ;
    }
   
    /*db persist*/
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey = spawn_latest_backup_id_key();
    string pval = to_string(m_ctx->latest_backup_id());
    transaction->put(pkey, pval);
    pkey = spawn_backup_attr_map_key(m_ctx->cur_backup_name());
    pval = spawn_backup_attr_map_val(it->second);
    transaction->put(pkey, pval);
    for(auto block : block_map){
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        pval = block.second;
        transaction->put(pkey, pval);
    }
    m_ctx->index_store()->submit_transaction(transaction);

    /*delete snapshot of prev backup*/
    string prev_backup = m_ctx->get_prev_backup(cur_backup);
    if(!prev_backup.empty() && m_ctx->get_backup_mode(prev_backup) == BackupMode::BACKUP_INCR){
        string prev_snap = backup_to_snap_name(prev_backup);
        if(!prev_snap.empty()){
            StatusCode ret = m_ctx->snap_client()->DeleteSnapshot(m_ctx->vol_name(), prev_snap);
            assert(ret == StatusCode::sOk);
        }
    } 
    
    m_task_status = TASK_DONE;
    m_ctx->trace();
}

DeleteBackupTask::DeleteBackupTask(const string& backup_name, shared_ptr<BackupCtx> ctx)
                 :BackupTask(backup_name, ctx)
{
    m_task_name.append(backup_name);
    m_task_name.append("_delete_backup");
    m_task_status = TASK_READY;
}

DeleteBackupTask::~DeleteBackupTask()
{
}

StatusCode DeleteBackupTask::do_delete_backup(const string& cur_backup)
{
    backupid_t backup_id = m_ctx->get_backup_id(cur_backup);
    assert(backup_id != -1);

    auto backup_it = m_ctx->cur_blocks_map().find(backup_id);
    assert(backup_it != m_ctx->cur_blocks_map().end());
    
    /*delete backup object*/
    auto block_map = backup_it->second;
    for(auto block_it : block_map){
        m_ctx->block_store()->remove(block_it.second); 
    }
    
    /*db persist*/
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey;
    for(auto block : block_map){
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

StatusCode DeleteBackupTask::do_merge_backup(const string& cur_backup, 
                                             const string& next_backup)
{
    backupid_t cur_backup_id  = m_ctx->get_backup_id(cur_backup);
    backupid_t next_backup_id = m_ctx->get_backup_id(next_backup);
    assert(cur_backup_id != -1 && next_backup_id != -1);

    auto cur_backup_it = m_ctx->cur_blocks_map().find(cur_backup_id);
    auto next_backup_it = m_ctx->cur_blocks_map().find(next_backup_id);
    assert(cur_backup_it != m_ctx->cur_blocks_map().end());
    assert(next_backup_it != m_ctx->cur_blocks_map().end());
    
    auto& cur_blocks_map  = cur_backup_it->second;
    auto& next_block_map = next_backup_it->second;
    for(auto cur_block_it : cur_blocks_map){
        auto ret = next_block_map.insert({cur_block_it.first, cur_block_it.second}); 
        if(!ret.second){
            /*backup block already in next bakcup, delete attached object*/ 
            m_ctx->block_store()->remove(cur_block_it.second);
        }
    }

     /*db persist*/
    IndexStore::Transaction transaction = m_ctx->index_store()->fetch_transaction();
    string pkey;
    string pval;
    for(auto block : cur_blocks_map){
        pkey = spawn_backup_block_map_key(cur_backup_id, block.first);
        transaction->del(pkey);
    }
    pkey = spawn_backup_attr_map_key(cur_backup);
    transaction->del(pkey);
    for(auto block : next_block_map){
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

void DeleteBackupTask::work()
{
   /* delete backup snapshot
    * snapshot may deleted repeated, no matter
    */
    string cur_backup = m_backup_name; 
    string backup_snap = backup_to_snap_name(cur_backup);
    m_ctx->snap_client()->DeleteSnapshot(m_ctx->vol_name(), backup_snap);

    BackupMode backup_mode = m_ctx->get_backup_mode(cur_backup);

    if(backup_mode == BackupMode::BACKUP_FULL){
        /*can directly delete */ 
        do_delete_backup(cur_backup);
    } else if(backup_mode == BackupMode::BACKUP_INCR){
        string next_backup = m_ctx->get_next_backup(cur_backup);
        if(next_backup.empty()){
            do_delete_backup(cur_backup);
        } else {
            BackupMode next_backup_mode = m_ctx->get_backup_mode(cur_backup);
            if(next_backup_mode == BackupMode::BACKUP_FULL){
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
