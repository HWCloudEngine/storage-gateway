#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "common/define.h"
#include "common/utils.h"
#include "log/log.h"
#include "rpc/snapshot.pb.h"
#include "backup_ctx.h"

BackupCtx::BackupCtx(const string& vol_name, const size_t& vol_size)
{
    m_vol_name = vol_name;
    m_vol_size = vol_size;
    m_latest_backup_id = BACKUP_INIT_UUID;
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

BackupCtx::~BackupCtx()
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

string BackupCtx::vol_name()const
{
    return m_vol_name;
}

size_t BackupCtx::vol_size()const
{
    return m_vol_size;
}

backupid_t BackupCtx::set_latest_backup_id(const backupid_t& backup_id)
{
    m_latest_backup_id = backup_id;
}

backupid_t BackupCtx::latest_backup_id()const
{
    return m_latest_backup_id;
}

map<string, backup_attr_t>& BackupCtx::cur_backups_map()
{
    return m_backups;
}

map<backupid_t, map<block_t, backup_object_t>>& BackupCtx::cur_blocks_map()
{
    return m_backup_block_map;
}

IndexStore* BackupCtx::index_store() const
{
    return m_index_store;
}

BlockStore* BackupCtx::block_store()const
{
    return m_block_store;
}

SnapshotCtrlClient* BackupCtx::snap_client()const
{
    return m_snap_client;
}

bool BackupCtx::is_incr_backup_allowable()
{
    /*if current exist no full backup, incr backup will be rejected*/
    string latest_full_backup = get_latest_full_backup();
    if(latest_full_backup.empty()){
        LOG_ERROR << "failed: incr backup should have base full backup";
        return false; 
    }

    string latest_backup = get_latest_backup();
    if(latest_backup.empty()){
        LOG_ERROR << "failed: incr backup hasn't prev backup";
        return false; 
    }
    BackupStatus latest_backup_status = get_backup_status(latest_backup);
    if(latest_backup_status != BackupStatus::BACKUP_AVAILABLE){
        LOG_ERROR << "failed: incr backup's prev backup not available";
        return false; 
    }
    /*check prev backup snapshot valid or not, if invalid, reject create backup*/
    string latest_backup_snap = backup_to_snap_name(latest_backup);
    if(latest_backup_snap.empty()){
        LOG_ERROR << "failed: incr backup's prev backup snapshot not exist";
        return false; 
    }
    return is_snapshot_valid(latest_backup_snap);
}

bool BackupCtx::is_backup_exist(const string& backup_name)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    if(it != m_backups.end()){
        return true;
    }
    return false;
}

bool BackupCtx::is_backup_deletable(const string& backup_name)
{
    /*check whether backup can be deleted*/
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto backup_it = m_backups.find(backup_name);
    if(backup_it == m_backups.end()){
        return false;
    }
    
    /*the current backup be restoring, can't delete */
    if(backup_it->second.backup_status == BackupStatus::BACKUP_RESTORING){
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

bool BackupCtx::is_snapshot_valid(const string& cur_snap)
{
    SnapStatus cur_snap_status;

    m_snap_client->QuerySnapshot(m_vol_name, cur_snap, cur_snap_status);
    if(cur_snap_status != SnapStatus::SNAP_CREATED){
        LOG_ERROR << "failed: snapshot:" << cur_snap << " not available";
        return false;
    }
    return true;
}

backupid_t BackupCtx::get_backup_id(const string& cur_backup)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(cur_backup);
    if(it != m_backups.end()){
        return it->second.backup_id; 
    }
    return -1;
}

string BackupCtx::get_backup_name(const backupid_t& backup_id)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    for(auto it : m_backups){
        if(it.second.backup_id == backup_id)
            return it.first;
    }
    return "";
}

BackupMode BackupCtx::get_backup_mode(const string& cur_backup)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(cur_backup);
    assert(it != m_backups.end());
    return it->second.backup_mode;
}

BackupStatus BackupCtx::get_backup_status(const string& cur_backup)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(cur_backup);
    assert(it != m_backups.end());
    return it->second.backup_status;
}

BackupType BackupCtx::get_backup_type(const string& cur_backup)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(cur_backup);
    assert(it != m_backups.end());
    return it->second.backup_type;
}


bool BackupCtx::update_backup_status(const string& cur_backup, const BackupStatus& backup_status)
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    auto it = m_backups.find(cur_backup);
    if(it == m_backups.end()){
        LOG_ERROR << " update backup status failed";
        return false; 
    }
    it->second.backup_status = backup_status;
    return true;
}

string BackupCtx::get_prev_backup(const string& cur_backup)
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

string BackupCtx::get_next_backup(const string& cur_backup)
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

string BackupCtx::get_latest_full_backup()
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

string BackupCtx::get_latest_backup()
{
    auto rit = m_backup_block_map.rbegin();
    if(rit == m_backup_block_map.rend()){
        return ""; 
    }

    backupid_t backup_id = rit->first;
    return get_backup_name(backup_id);
}

string BackupCtx::get_backup_base(const string& cur_backup)
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

backupid_t BackupCtx::spawn_backup_id()
{
    lock_guard<std::recursive_mutex> lock(m_mutex);
    /*todo: how to maintain and recycle backup id*/
    backupid_t backup_id = m_latest_backup_id;
    m_latest_backup_id++;
    return backup_id;
}

void BackupCtx::trace()
{
    LOG_INFO << "\t latest backup id:" << m_latest_backup_id;
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
