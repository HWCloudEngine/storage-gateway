#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <future>
#include "../log/log.h"
#include "../common/utils.h"
#include "../snapshot/snapshot_type.h"
#include "backup_util.h"
#include "backup_mds.h"

using huawei::proto::BackupStatus;
using huawei::proto::DiffBlocks;
using huawei::proto::RestoreBlocks;

#define BACKUP_UUID (2222)

BackupMds::BackupMds(const string& vol_name, const size_t& vol_size)
{
    m_vol_name = vol_name;
    m_vol_size = vol_size;
    m_latest_backup_id = BACKUP_UUID;
    m_backups.clear();
    m_backup_block_map.clear();

    m_block_store = new CephBlockStore();
    
    /*todo: read from configure file*/
    string db_path = DB_DIR + vol_name + "/backup";
    if(access(db_path.c_str(), F_OK)){
        char cmd[256] = "";
        sprintf(cmd, "mkdir -p %s", db_path.c_str());
        int ret = system(cmd);
        assert(ret != -1);
    }
    m_index_store = IndexStore::create("rocksdb", db_path);
    m_index_store->db_open();

    m_snap_client = new SnapshotCtrlClient(grpc::CreateChannel("127.0.0.1:1111", 
                    grpc::InsecureChannelCredentials()));
}

BackupMds::~BackupMds()
{
    if(m_snap_client){
        delete m_snap_client; 
    }
    if(m_index_store){
        delete m_index_store;
    }
    if(m_block_store){ 
        delete m_block_store;
    }
    m_backup_block_map.clear();
    m_backups.clear();
}

StatusCode BackupMds::create_backup(const CreateBackupInReq* req, CreateBackupInAck* ack)
{
    string vol_name    = req->vol_name();
    string backup_name = req->backup_name();
    BackupMode backup_mode = req->backup_option().backup_mode();

    LOG_INFO << "create backup vname:" << vol_name << " bname:" << backup_name;

    /*check whether backup exist*/
    if(is_backup_exist(backup_name)){
        ack->set_status(StatusCode::sBackupAlreadyExist);
        LOG_INFO << "create backup vname:" << vol_name <<" bname:" 
                 << backup_name <<"failed already exist";
        return StatusCode::sBackupAlreadyExist; 
    }
    
    /*check whether incr backup allowable*/
    if(backup_mode == BackupMode::BACKUP_INCR){
        if(!is_incr_backup_allowable()){
            LOG_INFO << "create backup vname:" << vol_name <<" bname:" 
                     << backup_name <<" failed incr has no full backup exist";
            return StatusCode::sBackupNoFullbackupExist;
        } 
    }
    
    /*create backup*/
    backup_attr_t cur_backup_attr;
    cur_backup_attr.volume_uuid = vol_name;
    cur_backup_attr.backup_mode = req->backup_option().backup_mode();
    cur_backup_attr.backup_name = req->backup_name();
    /*backup_id generated when backup is created*/
    cur_backup_attr.backup_status = BackupStatus::BACKUP_CREATING;

    /*in memroy update backup status*/
    m_backups.insert({backup_name, cur_backup_attr});
    /*record current active backup*/
    m_cur_backup = backup_name;

    /*db persist */
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey = spawn_latest_backup_name_key();
    transaction->put(pkey, m_cur_backup);
    pkey = spawn_backup_attr_map_key(m_cur_backup);
    string pval = spawn_backup_attr_map_val(cur_backup_attr);
    transaction->put(pkey, pval);
    m_index_store->submit_transaction(transaction);

    /*start async backup create task*/
    //auto future = std::async(std::launch::async, std::bind(&BackupMds::async_create_backup, this));
    thread async_task(std::bind(&BackupMds::async_create_backup, this));
    async_task.detach();

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "create backup vname:" << vol_name <<" bname:" << backup_name <<" ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::list_backup(const ListBackupInReq* req, ListBackupInAck* ack)
{
    string vol_name = req->vol_name();
    LOG_INFO << "list backup vname:" << vol_name;
   
    /*return backup names*/
    lock_guard<std::recursive_mutex> lock(m_mutex);
    for(auto it : m_backups){
        ack->add_backup_name(it.first);
    }
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "list backup vname:" << vol_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::get_backup(const GetBackupInReq* req, GetBackupInAck* ack)
{
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "query backup vname:" << vol_name << " bname:" << backup_name;
    BackupStatus backup_status = get_backup_status(backup_name);
    ack->set_status(StatusCode::sOk);
    LOG_INFO << "query backup vname:" << vol_name << " bname:" << backup_name 
             << " bstatus:" << backup_status << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::delete_backup(const DeleteBackupInReq* req, DeleteBackupInAck* ack)
{
    string vol_name    = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name;

    /*check whether backup exist*/
    if(!is_backup_exist(backup_name)){
        ack->set_status(StatusCode::sBackupNotExist);
        LOG_INFO << "delete backup vname:" << vol_name <<" bname:" 
                 << backup_name <<"failed not exist";
        return StatusCode::sBackupNotExist; 
    }

    /*check whether backup can be delete*/
    if(!is_backup_deletable(backup_name)){
        LOG_INFO << "delete backup vname:" << vol_name <<" bname:" 
                 << backup_name <<" failed can not delete";
        return StatusCode::sBackupCanNotDelete; 
    }

    /*delete backup*/
    {
        lock_guard<std::recursive_mutex> lock(m_mutex);
        auto it = m_backups.find(backup_name);
        assert(it != m_backups.end());
        /*in memroy update backup status*/
        it->second.backup_status = BackupStatus::BACKUP_DELETING; 
        /*(todo)in db update snapshot status*/
    }
    
    /*start async delete stask*/
    //auto future = std::async(std::launch::async, std::bind(&BackupMds::async_delete_backup, this, backup_name));
    thread async_task(std::bind(&BackupMds::async_delete_backup, this, backup_name));
    async_task.detach();

    ack->set_status(StatusCode::sOk);
    LOG_INFO << "delete backup vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::restore_backup(const RestoreBackupInReq* req, RestoreBackupInAck* ack)
{
    string vol_name = req->vol_name();
    string backup_name = req->backup_name();
    LOG_INFO << "restore backup vname:" << vol_name << " bname:" << backup_name;
    string backup_base = get_backup_base(backup_name);
    string cur_backup  = backup_base;

    while(!cur_backup.empty()){
        backupid_t cur_backup_id = get_backup_id(cur_backup);
        auto cur_backup_block_map_it = m_backup_block_map.find(cur_backup_id);
        assert(cur_backup_block_map_it != m_backup_block_map.end());
        auto cur_backup_block_map = cur_backup_block_map_it->second;
        RestoreBlocks* restore_blocks = ack->add_restore_blocks();
        restore_blocks->set_vol_name(vol_name);
        restore_blocks->set_backup_name(cur_backup);
        for(auto cur_block : cur_backup_block_map){
            restore_blocks->add_block_no(cur_block.first);
            restore_blocks->add_block_obj(cur_block.second);
        }

        if(cur_backup.compare(backup_name) == 0){
            /*arrive the end backup*/
            break;
        }
    }

    LOG_INFO << "restore backup vname:" << vol_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk;
}

backupid_t BackupMds::spawn_backup_id()
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    /*todo: how to maintain and recycle backup id*/
    backupid_t backup_id = m_latest_backup_id;
    m_latest_backup_id++;
    return backup_id;
}

backupid_t BackupMds::get_backup_id(const string& backup_name)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    if(it != m_backups.end()){
        return it->second.backup_id; 
    }
    return -1;
}

string BackupMds::get_backup_name(const backupid_t& backup_id)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    for(auto it : m_backups){
        if(it.second.backup_id == backup_id)
            return it.first;
    }
    return "";
}

BackupMode BackupMds::get_backup_mode(const string& backup_name)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    assert(it != m_backups.end());
    return it->second.backup_mode;
}

BackupStatus BackupMds::get_backup_status(const string& backup_name)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    assert(it != m_backups.end());
    return it->second.backup_status;
}

string BackupMds::get_prev_backup(const string& cur_backup)
{
    backupid_t cur_backup_id = get_backup_id(cur_backup);
    assert(cur_backup_id != -1);

    auto cur_backup_it = m_backup_block_map.find(cur_backup_id);
    assert(cur_backup_it != m_backup_block_map.end());
    if(cur_backup_it == m_backup_block_map.begin()){
        LOG_ERROR << "cur_backup:" << cur_backup << " has no prev backup"; 
        return "";
    }
    auto prev_backup_it = std::prev(cur_backup_it);
    backupid_t prev_backup_id = prev_backup_it->first;
    return get_backup_name(prev_backup_id);
}

string BackupMds::get_next_backup(const string& cur_backup)
{
    backupid_t cur_backup_id = get_backup_id(cur_backup);
    assert(cur_backup_id != -1);

    auto cur_backup_it = m_backup_block_map.find(cur_backup_id);
    if(cur_backup_it == m_backup_block_map.end()){
        LOG_ERROR << "cur_backup:" << cur_backup << " has no next backup"; 
        return "";
    }

    auto next_backup_it = std::next(cur_backup_it);
    if(next_backup_it == m_backup_block_map.end()){
        LOG_ERROR << "cur_backup:" << cur_backup << " has no next backup"; 
        return ""; 
    }

    backupid_t next_backup_id = next_backup_it->first;
    return get_backup_name(next_backup_id);
}

string BackupMds::spawn_backup_object_name(const backupid_t& backup_id, 
                                           const block_t&    blk_id)
{
    /*todo: may add pool name*/
    string backup_object_name = m_vol_name;
    backup_object_name.append(BACKUP_FS);
    backup_object_name.append(to_string(backup_id));
    backup_object_name.append(BACKUP_FS);
    backup_object_name.append(to_string(blk_id));
    backup_object_name.append(BACKUP_OBJ_SUFFIX);
    return backup_object_name;
}

void BackupMds::split_backup_object_name(const string& raw, 
                                         const string& vol_name,
                                         const backupid_t& backup_id, 
                                         const block_t&  blk_id)
{
    /*todo */
    return;
}

StatusCode BackupMds::do_full_backup()
{
    /*1. async read current snapshot*/
    StatusCode ret_code = StatusCode::sOk;
    off_t  cur_pos = 0;
    off_t  end_pos = m_vol_size;
    size_t chunk_size = BACKUP_BLOCK_SIZE;
    char*  chunk_buf  = (char*)malloc(chunk_size);
    assert(chunk_buf != nullptr);

    while(cur_pos < end_pos)
    {
        chunk_size =((end_pos-cur_pos) > BACKUP_BLOCK_SIZE) ?  \
                      BACKUP_BLOCK_SIZE : (end_pos-cur_pos);
        
        /*current backup corespondent snapshot*/
        string cur_snap = backup_to_snap_name(m_cur_backup);
        ret_code = m_snap_client->ReadSnapshot(m_vol_name, cur_snap, 
                                               chunk_buf, chunk_size, cur_pos);
        assert(ret_code == StatusCode::sOk);

        block_t cur_blk_no = (cur_pos / BACKUP_BLOCK_SIZE);
        backupid_t cur_backup_id = get_backup_id(m_cur_backup);
        backup_object_t cur_blk_obj = spawn_backup_object_name(cur_backup_id, cur_blk_no);
        
        /*store backup data to block store in object*/
        int write_ret = m_block_store->write(cur_blk_obj, chunk_buf, chunk_size, 0);
        assert(write_ret == 0);

        /*update backup block map*/
        auto cur_backup_block_map_it = m_backup_block_map.find(cur_backup_id);
        assert(cur_backup_block_map_it != m_backup_block_map.end());
        map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
        cur_backup_block_map.insert({cur_blk_no, cur_blk_obj});
        
        cur_pos += chunk_size;
    }
    
    if(chunk_buf){
        free(chunk_buf); 
    }

    return ret_code;
}

StatusCode BackupMds::do_incr_backup()
{
    StatusCode ret_code = StatusCode::sOk;
    string prev_backup = get_prev_backup(m_cur_backup);
    if(prev_backup.empty()){
        LOG_ERROR << "incr backup:" << m_cur_backup << "has no previous backup";
        return StatusCode::sBackupNotExist;
    }

    /*1. diff cur and prev snapshot*/
    string pre_snap = backup_to_snap_name(prev_backup); 
    string cur_snap = backup_to_snap_name(m_cur_backup);
    vector<DiffBlocks> diff_blocks;
    ret_code = m_snap_client->DiffSnapshot(m_vol_name, pre_snap, cur_snap, diff_blocks); 
    assert(ret_code == StatusCode::sOk);

    /*2. read diff data(current snapshot and diff region)*/
    char* buf = (char*)malloc(COW_BLOCK_SIZE);
    for(auto it : diff_blocks){
        uint64_t diff_block_no_size = it.diff_block_no_size() ;
        for(int i = 0; i < diff_block_no_size; i++){
            uint64_t diff_block_no = it.diff_block_no(i);
            off_t    diff_block_off = diff_block_no * COW_BLOCK_SIZE;
            size_t   diff_block_size = COW_BLOCK_SIZE;

            ret_code = m_snap_client->ReadSnapshot(m_vol_name, cur_snap, buf, 
                                                   diff_block_size, diff_block_off);
            assert(ret_code == StatusCode::sOk);

            /*3. append backup meta(block, object) and (object, backup_ref)*/
            block_t cur_backup_blk_no = (diff_block_off / BACKUP_BLOCK_SIZE);
            backupid_t cur_backup_id  = get_backup_id(m_cur_backup);
            backup_object_t cur_backup_blk_obj = spawn_backup_object_name(cur_backup_id, cur_backup_blk_no);

            /*store backup data to block store in object*/
            size_t write_size = diff_block_size;
            /*snapshot cow block  unit diff from backup block unit*/
            off_t  write_off = (diff_block_off % BACKUP_BLOCK_SIZE);
            int write_ret = m_block_store->write(cur_backup_blk_obj, buf, write_size, write_off);
            assert(write_ret == 0);

            /*update backup block map*/
            auto cur_backup_block_map_it = m_backup_block_map.find(cur_backup_id);
            assert(cur_backup_block_map_it != m_backup_block_map.end());
            map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
            cur_backup_block_map.insert({cur_backup_blk_no, cur_backup_blk_obj});
        }
    }
    
    if(buf){
        free(buf);
    }

    return ret_code;
}

bool BackupMds::is_backup_exist(const string& backup_name)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    if(it != m_backups.end()){
        return true;
    }
    return false;
}

bool BackupMds::is_incr_backup_allowable()
{
    /*if current exist no full backup, incr backup will be rejected*/
    string latest_full_backup = get_latest_full_backup();
    if(latest_full_backup.empty()){
        return false; 
    }
    return true;
}

bool BackupMds::is_backup_deletable(const string& backup_name)
{
    /*check whether backup can be deleted*/
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto backup_it = m_backups.find(backup_name);
    if(backup_it == m_backups.end()){
        return false;
    }
    
    /*any incr backup can be deleted*/
    if(backup_it->second.backup_mode == BackupMode::BACKUP_INCR){
        return true; 
    }

    if(backup_it->second.backup_mode == BackupMode::BACKUP_FULL){
        string next_backup_name = get_next_backup(backup_name);
        if(!next_backup_name.empty()){
            auto next_backup_it = m_backups.find(next_backup_name);
            if(next_backup_it != m_backups.end()){
                if(next_backup_it->second.backup_mode == BackupMode::BACKUP_INCR){
                    /*current full bakcup has incr backup base on it*/
                    return false; 
                } 
            }
        }
    }

    return true;
}

StatusCode BackupMds::async_create_backup()
{   
    auto it = m_backups.find(m_cur_backup);
    if(it == m_backups.end()){
        LOG_ERROR << "cur_backup:" << m_cur_backup << " no exist";
        return StatusCode::sBackupNotExist; 
    }

    /*query snapshot of current backup whether created*/
    string cur_backup_snap = backup_to_snap_name(m_cur_backup);
    /*todo times limited or timeout*/
    while(true){
        SnapStatus cur_backup_snap_status;
        m_snap_client->QuerySnapshot(m_vol_name, cur_backup_snap, cur_backup_snap_status);
        if(cur_backup_snap_status == SnapStatus::SNAP_CREATED){
            break;
        }
        usleep(200);
    }

    /*generate backup id*/
    backupid_t backup_id  = spawn_backup_id();

    /*update backup attr*/
    it->second.backup_id     = backup_id;
    it->second.backup_status = BackupStatus::BACKUP_CREATED;

    /*prepare backup block map*/
    map<block_t, backup_object_t> block_map;
    m_backup_block_map.insert({backup_id, block_map});
    
    /*do backup and update backup block map*/
    if(it->second.backup_mode == BackupMode::BACKUP_FULL){
        do_full_backup();
    } else if(it->second.backup_mode == BackupMode::BACKUP_INCR){
        do_incr_backup();
    } else {
        ;
    }
    
    /*db persist*/
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey = spawn_latest_backup_id_key();
    string pval = to_string(m_latest_backup_id);
    transaction->put(pkey, pval);
    pkey = spawn_backup_attr_map_key(m_cur_backup);
    pval = spawn_backup_attr_map_val(it->second);
    transaction->put(pkey, pval);
    for(auto block : block_map){
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        pval = block.second;
        transaction->put(pkey, pval);
    }
    m_index_store->submit_transaction(transaction);

    /*delete snapshot of prev backup*/
    string prev_backup = get_prev_backup(m_cur_backup);
    if(!prev_backup.empty() && get_backup_mode(prev_backup) == BackupMode::BACKUP_INCR){
        string prev_snap = backup_to_snap_name(prev_backup);
        if(!prev_snap.empty()){
            StatusCode ret = m_snap_client->DeleteSnapshot(m_vol_name, prev_snap);
            assert(ret == StatusCode::sOk);
        }
    } 
    
    trace();

    return StatusCode::sOk;
}

StatusCode BackupMds::do_delete_backup(const string& backup_name)
{
    backupid_t backup_id = get_backup_id(backup_name);
    assert(backup_id != -1);

    auto backup_it = m_backup_block_map.find(backup_id);
    assert(backup_it != m_backup_block_map.end());
    
    /*delete backup object*/
    auto block_map = backup_it->second;
    for(auto block_it : block_map){
        m_block_store->remove(block_it.second); 
    }
    
    /*db persist*/
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey;
    for(auto block : block_map){
        pkey = spawn_backup_block_map_key(backup_id, block.first);
        transaction->del(pkey);
    }
    pkey = spawn_backup_attr_map_key(backup_name);
    transaction->del(pkey);
    m_index_store->submit_transaction(transaction);

    /*delete backup meta in memory*/
    block_map.clear();
    m_backup_block_map.erase(backup_id);
    m_backups.erase(backup_name);
    
    return StatusCode::sOk;
}

StatusCode BackupMds::do_merge_backup(const string& cur_backup, const string& next_backup)
{
    backupid_t cur_backup_id = get_backup_id(cur_backup);
    backupid_t next_backup_id = get_backup_id(next_backup);
    assert(cur_backup_id != -1 && next_backup_id != -1);

    auto cur_backup_it = m_backup_block_map.find(cur_backup_id);
    auto next_backup_it = m_backup_block_map.find(next_backup_id);
    assert(cur_backup_it != m_backup_block_map.end());
    assert(next_backup_it != m_backup_block_map.end());
    
    auto& cur_block_map  = cur_backup_it->second;
    auto& next_block_map = next_backup_it->second;
    for(auto cur_block_it : cur_block_map){
        auto ret = next_block_map.insert({cur_block_it.first, cur_block_it.second}); 
        if(!ret.second){
            /*backup block already in next bakcup, delete attached object*/ 
            m_block_store->remove(cur_block_it.second);
        }
    }

     /*db persist*/
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey;
    string pval;
    for(auto block : cur_block_map){
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
    m_index_store->submit_transaction(transaction);

   
    /*delete backup meta in memory*/
    cur_block_map.clear();
    m_backup_block_map.erase(cur_backup_id);
    m_backups.erase(cur_backup);

    return StatusCode::sOk;
}

string BackupMds::get_latest_full_backup()
{
    auto rit = m_backup_block_map.rbegin();
    for(; rit != m_backup_block_map.rend(); ++rit){
        backupid_t backup_id = rit->first;
        string backup_name = get_backup_name(backup_id);
        BackupMode backup_mode = get_backup_mode(backup_name);
        if(backup_mode == BackupMode::BACKUP_FULL){
            return backup_name;
        }
    }
    return "";
}

string BackupMds::get_backup_base(const string& cur_backup)
{
    BackupMode cur_backup_mode = get_backup_mode(cur_backup);
    if(cur_backup_mode == BackupMode::BACKUP_FULL){
        return cur_backup;
    } else {
        string prev_backup = get_prev_backup(cur_backup);
        while(!prev_backup.empty()){
            BackupMode prev_backup_mode = get_backup_mode(prev_backup);
            if(prev_backup_mode == BackupMode::BACKUP_FULL){
                return prev_backup;
            }
            prev_backup = get_prev_backup(prev_backup);
        }
    }
    return "";
}

StatusCode BackupMds::async_delete_backup(const string& backup_name)
{ 
    /* delete backup snapshot
     * snapshot may deleted repeatedy, no matter
     */
    string backup_snap = backup_to_snap_name(backup_name);
    m_snap_client->DeleteSnapshot(m_vol_name, backup_snap);

    BackupMode backup_mode = get_backup_mode(backup_name);

    if(backup_mode == BackupMode::BACKUP_FULL){
        /*can directly delete */ 
        return do_delete_backup(backup_name);
    } else if(backup_mode == BackupMode::BACKUP_INCR){
        string next_backup = get_next_backup(backup_name);
        if(next_backup.empty()){
            do_delete_backup(backup_name);
        } else {
            BackupMode next_backup_mode = get_backup_mode(next_backup);
            if(next_backup_mode == BackupMode::BACKUP_FULL){
                /*can directly delete*/ 
                do_delete_backup(backup_name);
            } else {
                /*merge with next backup and then delete*/ 
                do_merge_backup(backup_name, next_backup);
            }
        }
    } 
    
    trace();

    return StatusCode::sOk;
}

int BackupMds::recover()
{
    IndexStore::SimpleIteratorPtr it = m_index_store->db_iterator();
    
    /*recover latest backup id*/
    string prefix = BACKUP_ID_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
          it->next()){
        string value = it->value();
        m_latest_backup_id = atol(value.c_str());
    } 
    
    /*recover latest backup name*/
    prefix = BACKUP_NAME_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
          it->next()){
        string value = it->value();
        m_cur_backup = value;
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

        m_backups.insert({backup_name, backup_attr});
    } 
  
    /*recover backup block map*/
    for(auto backup : m_backups){
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
        
        m_backup_block_map.insert({backup_id, block_map});
    }
    
    trace();

    LOG_INFO << "drserver recover backup meta data ok";
    return 0;
}

void BackupMds::trace()
{
    LOG_INFO << "\t latest backup id:" << m_latest_backup_id;
    LOG_INFO << "\t current backup:"   << m_cur_backup;
    for(auto it : m_backups){
        LOG_INFO << "\t\t backup_name:" << it.first 
                 << " backup_id:"       << it.second.backup_id
                 << " backup_status:"   << it.second.backup_status;
    }

    LOG_INFO << "\t backup block map";
    for(auto it : m_backup_block_map){
        LOG_INFO << "\t\t backup_id:" << it.first;
        map<block_t, backup_object_t>& blk_map = it.second;
        for(auto blk_it : blk_map){
            LOG_INFO << "\t\t\t blk_no:" << blk_it.first 
                     << " backup_obj:"   << blk_it.second;
        }
    }
}
