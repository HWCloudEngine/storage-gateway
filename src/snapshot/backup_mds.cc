#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <future>
#include "snapshot_util.h"
#include "backup_mds.h"

using huawei::proto::control::BackupStatus;
using huawei::proto::inner::DiffBlocks;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;

BackupMds::BackupMds(SnapshotMds* snap_mds)
{
    m_cur_backup_id = 0;
    m_backups.clear();
    m_backup_block_map.clear();
    m_backup_object_map.clear();

    m_block_store = new CephBlockStore();

    /*todo: read from configure file*/
    string db_path = DB_DIR + vol_name + "/backup";
    if(access(db_path.c_str(), F_OK)){
        int ret = mkdir(db_path.c_str(), 0777); 
        assert(ret == 0);
    }

    m_index_store = IndexStore::create("rocksdb", db_path);
    m_index_store->db_open();

    m_snap_client = new SnapCtrlClient(grpc::CreateChannel("127.0.0.1:1111", 
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
    m_backup_object_map.clear();
    m_backup_block_map.clear();
    m_backups.clear();
}

StatusCode BackupMds::sync(const SyncReq* req, SyncAck* ack)
{
    //string vol_name = req->vol_name();
    //LOG_INFO << "BackupMds sync" << " vname:" << vol_name;
    //lock_guard<std::mutex> lock(m_mutex);
    //ack->mutable_header()->set_status(StatusCode::sOk);
    //ack->set_latest_snap_name(m_latest_snapname); 
    //LOG_INFO << "BackupMds sync" << " vname:" << vol_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::create_snapshot(const CreateReq* req, CreateAck* ack)
{
    string vol_name    = req->vol_name();
    string snap_name   = req->snap_name();
    string backup_name = req->backup_name();

    LOG_INFO << "create snapshot vname:" << vol_name << " sname:" << snap_name
             << " bname:" << backup_name;
    
    /*1. create snapshot*/
    m_snap_mds->create_snapshot(req, ack);
    
    /*2. create backup*/
    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    if(it != m_backups.end()){
        ack->mutable_header()->set_status(StatusCode::sBackupAlreadyExist);
        LOG_INFO << "create backup vname:" << vol_name <<" bname:" 
                 << backup_name <<" already exist";
        return StatusCode::sBackupAlreadyExist; 
    }
    
    backup_attr_t cur_backup_attr;
    cur_backup_attr.replication_uuid = req->header().replication_uuid();
    cur_backup_attr.checkpoint_uuid  = req->header().checkpoint_uuid();
    cur_backup_attr.volume_uuid = vol_name;
    cur_backup_attr.backup_name = req->backup_name();
    cur_bakcup_attr.backup_mode = req->backup_mode();
    cur_bakcup_attr.backup_store_mode = req->backup_store_mode();
    cur_backup_attr.backup_option = req->backup_option();
    cur_backup_attr.backup_status = BackupStatus::BACKUP_CREATING;

    /*in db update backup status*/
    //string pkey = DbUtil::spawn_attr_map_key(snap_name);
    //string pval = DbUtil::spawn_attr_map_val(cur_snap_attr);
    //m_index_store->db_put(pkey, pval);

    /*in memroy update backup status*/
    m_backups.insert({backup_name, cur_backup_attr});

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "create snapshot vname:" << vol_name <<" sname:" << snap_name <<" ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::list_snapshot(const ListReq* req, ListAck* ack)
{
    string vol_name = req->vol_name();
    LOG_INFO << "list snapshot vname:" << vol_name;
   
    /*return backup names*/
    lock_guard<std::mutex> lock(m_mutex);
    for(auto it : m_backups){
        ack->add_backup_name(it.first);
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "list snapshot vname:" << vol_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::query_snapshot(const QueryReq* req, QueryAck* ack)
{
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    string backup_name = req->backup_name();
    LOG_INFO << "query snapshot vname:" << vol_name << " sname:" << snap_name
             << " bname:" << backup_name;

    lock_guard<std::mutex> lock(m_mutex);
    StatusCode ret = StatusCode::sOk;
    do {
        auto it = m_backups.find(snap_name);
        if(it == m_backups.end()){
            ret = StatusCode::sBackupNotExist;
            break;
        }
        ack->set_backup_status(it->second.backup_status);
    }while(0);

    ack->mutable_header()->set_status(ret);
    LOG_INFO << "query snapshot vname:" << vol_name << " sname:" << snap_name << " ok";
    return ret; 
}

StatusCode BackupMds::delete_snapshot(const DeleteReq* req, DeleteAck* ack)
{
    string vol_name    = req->vol_name();
    string snap_name   = req->snap_name();
    string backup_name = req->backup_name();
    LOG_INFO << "delete snapshot vname:" << vol_name << " sname:" << snap_name
             << " bname:" << backup_name;
    
    /*1.delete snapshot*/
    m_snap_mds->delete_snapshot(req, ack);
    
    /*2.delete backup*/
    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_backups.find(backup_name);
    if(it == m_backups.end()){
        ack->mutable_header()->set_status(StatusCode::sBackupNotExist);
        LOG_INFO << "delete snapshot vname:" << vol_name 
                 << " sname:" << snap_name 
                 << " bname:" << backup_name << " already exist";
        return StatusCode::sBackupNotExist;
    }
    
    BackupStatus cur_backup_status = BackupStatus::BACKUP_DELETING;
    /*in memroy update backup status*/
    it->second.backup_status = cur_backup_status;

    /*in db update snapshot status*/
    //string pkey = DbUtil::spawn_attr_map_key(snap_name);
    //string pval = DbUtil::spawn_attr_map_val(it->second);
    //m_index_store->db_put(pkey, pval);

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "delete snapshot vname:" << vol_name 
             << " sname:" << snap_name << " bname:" << backup_name << " ok";
    return StatusCode::sOk; 
}

StatusCode BackupMds::rollback_snapshot(const RollbackReq* req, RollbackAck* ack)
{
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "rollback snapshot vname:" << vol_name << " snap_name:" << snap_name;
 
    lock_guard<std::mutex> lock(m_mutex);
    string cur_snap_name = mapping_snap_name(req->header(), snap_name); 
    if(cur_snap_name.empty()){
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
            << " snap_name:" << snap_name << "not exist";
        return StatusCode::sSnapNotExist;
    }

    auto it = m_snapshots.find(cur_snap_name);
    if(it == m_snapshots.end()){
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
            << " snap_name:" << snap_name << "not exist";
        return StatusCode::sSnapNotExist;
    }

    snapid_t snap_id = it->second.snap_id;
    auto snap_it = m_cow_block_map.find(snap_id);
    if(snap_it == m_cow_block_map.end()){
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
                 << " snap_id:" << snap_id << " not exist";
        return StatusCode::sSnapNotExist;
    }

    map<block_t, cow_object_t>& block_map = snap_it->second;
    for(auto blk_it : block_map)
    {
        RollBlock* roll_blk = ack->add_roll_blocks();
        roll_blk->set_blk_no(blk_it.first);
        roll_blk->set_blk_object(blk_it.second);
    } 

    BackupStatus cur_snap_status = BackupStatus::BACKUP_ROLLBACKING;
    m_snapshots[cur_snap_name].snap_status = cur_snap_status;

    string pkey = DbUtil::spawn_attr_map_key(cur_snap_name);
    string pval = DbUtil::spawn_attr_map_val(m_snapshots[cur_snap_name]);
    m_index_store->db_put(pkey, pval);

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "rollback snapshot vname:" << vol_name
             << " snap_name:" << snap_name << " ok";
    return StatusCode::sOk;
}

backupid_t BackupMds::spawn_backup_id()
{
    lock_guard<std::mutex> lock(m_mutex);
    /*todo: how to maintain and recycle backup id*/
    backupid_t backup_id = m_latest_backupid;
    m_latest_backupid++;
    return backup_id;
}

backupid_t BackupMds::get_backup_id(string backup_name)
{
    auto it = m_backups.find(backup_name);
    if(it != m_backups.end()){
        return it->second.backup_id; 
    }
    return -1;
}

string BackupMds::get_backup_name(backupid_t backup_id)
{
    for(auto it : m_backups){
        if(it.second.snap_id == backup_id)
            return it.first;
    }
    return nullptr;
}

string BackupMds::spawn_backup_object_name(const backupid_t backup_id, 
                                           const block_t    blk_id)
{
    /*todo: may add pool name*/
    string backup_object_name = m_vol_name;
    backup_object_name.append(FS);
    backup_object_name.append(to_string(backup_id));
    backup_object_name.append(FS);
    backup_object_name.append(to_string(blk_id));
    backup_object_name.append(OBJ_SUFFIX);
    return backup_object_name;
}

void BackupMds::split_backup_object_name(const string& raw, 
                                         string& vol_name,
                                         backupid_t& backup_id, 
                                         block_t&  blk_id)
{
    /*todo */
    return;
}

StatusCode BackupMds::full_backup()
{
    /*1. async read current snapshot*/
    off_t  cur_pos = 0;
    off_t  end_pos = m_vol_size;
    size_t chunk_size = DEFAULT_BACKUP_CHUNK_SIZE;
    char*  chunk_buf  = (char*)malloc(chunk_size);
    assert(chunk_buf != nullptr);

    while(cur_pos < end_pos)
    {
        chunk_size = std::min(DEFAULT_BACKUP_CHUNK_SIZE, (end_pos-cur_pos)); 
        
        /*current backup corespondent snapshot*/
        string m_cur_snap = m_cur_backup + ".snap";
        m_snap_client->ReadSnapshot(m_vol_name, m_cur_snap, 
                                    chunk_buf, chunk_size, cur_pos);
        
        block_t cur_blk_no = (cur_pos / DEFAULT_BACKUP_CHUNK_SIZE);
        backup_object_t cur_blk_obj = spawn_backup_object_name(m_cur_backup_id, cur_blk_no);
        
        /*store backup data to block store in object*/
        int ret = m_block_store->write(cur_blk_obj, chunk_buf, chunk_size, 0);
        assert(ret == 0);

        /*update backup block map*/
        auto cur_backup_block_map_it = m_backup_block_map.find(m_cur_backup_id);
        assert(cur_backup_block_map_it != m_backup_block_map.end());
        map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
        cur_backup_block_map.insert({cur_blk_no, cur_blk_obj});
        
        /*update backup block obj ref*/
        backup_object_ref_t cur_blk_obj_ref;
        cur_blk_obj_ref.insert(m_cur_backup_id);
        m_backup_object_map.insert({cur_blk_obj, cur_blk_obj_ref});
        
        cur_pos += chunk_size;
    }
    
    return StatusCode::sOk;
}

StatusCode BackupMds::incr_backup()
{
    /*1. diff cur and prev snapshot*/
    string m_pre_snap = m_pre_backup + ".snap";
    string m_cur_snap = m_cur_backup + ".snap";

    m_snap_client->DiffSnapshot()
    /*2. read diff data(current snapshot and diff region)*/
    /*3. append backup meta(block, object) and (object, backup_ref)*/
}

StatusCode BackupMds::async_create_backup()
{   
    auto it = m_backups.find(m_cur_backup);
    if(it == m_backups.end()){
        return StatusCode::sBackupNotExist; 
    }
    
    /*generate backup id*/
    backupid_t backup_id  = spawn_backup_id();

    /*update backup attr*/
    it->second.backup_id     = backup_id;
    it->second.backup_status = BackupStatus::BACKUP_CREATED;

    /*prepare backup block map*/
    map<block_t, backup_object_t> block_map;
    m_backup_block_map.insert({backup_id, block_map});
    
    if(it->second.backup_mode == BACKUP_FULL){
    } else if(it->second.backup_mode == BACKUP_INCR){
        if(m_latest_full_backup.empty()) {
            return StatusCode::sBackupNoFullbackupExist; 
        }
    } else {
    }

    /* delete prev snapshot*/
    return StatusCode::sOk;
}

StatusCode BackupMds::async_delete_backup()
{
    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_snapshots.find(snap_name);
    if(it == m_snapshots.end()){
        return StatusCode::sSnapNotExist; 
    }
    snapid_t snap_id = it->second.snap_id;
    auto snap_it = m_cow_block_map.find(snap_id);
    if(snap_it == m_cow_block_map.end()){
        return StatusCode::sSnapNotExist;
    }

    map<block_t, cow_object_t>& block_map = snap_it->second;
    
    /*-----transction begin-----*/
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey;
    string pval;
    /*delete cow block in the snapshot*/
    for(auto blk_it : block_map){
        /*in db delete cow block key*/
        pkey = DbUtil::spawn_cow_block_map_key(snap_id, blk_it.first);
        transaction->del(pkey);

        cow_object_t cow_obj = blk_it.second; 

        /*in db read modify write cow object reference*/
        pkey = DbUtil::spawn_cow_object_map_key(cow_obj);
        pval = m_index_store->db_get(pkey);
        cow_object_ref_t obj_ref;
        DbUtil::split_cow_object_map_val(pval, obj_ref);
        obj_ref.erase(snap_id);
        pval = DbUtil::spawn_cow_object_map_val(obj_ref);
        transaction->put(pkey, pval);
        
        if(obj_ref.empty()){
            transaction->del(pkey);
        }
    }
    pkey = DbUtil::spawn_attr_map_key(snap_name);
    transaction->del(pkey);
    int ret = m_index_store->submit_transaction(transaction);
    if(ret){
        return StatusCode::sSnapMetaPersistError; 
    }
    /*-----transction end-----*/

    for(auto blk_it : block_map){
        cow_object_t cow_obj = blk_it.second;

        /*in memory update cow object reference*/
        cow_object_ref_t& cow_obj_ref = m_cow_object_map[cow_obj];
        cow_obj_ref.erase(snap_id);
        
        if(cow_obj_ref.empty()){
            /*in mem delete cow object key*/
            m_cow_object_map.erase(cow_obj); 
            /*block store reclaim the cow object*/
            m_block_store->remove(cow_obj);
        }
    }
    block_map.clear();
    m_cow_block_map.erase(snap_id);
    m_snapshots.erase(snap_name);
    /*update latest snaphsot*/
    if(m_latest_snapname.compare(snap_name)){
        m_latest_snapname.clear();
    }

    trace();

    LOG_INFO << "deleted sname:" << snap_name << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::update(const UpdateReq* req, UpdateAck* ack)
{
    string vol_name    = req->vol_name();
    string snap_name   = req->snap_name();

    LOG_INFO << "update vname:" << vol_name << " sname:" << snap_name;
    
    /*1. update snapshot*/
    m_snap_mds->update(req, ack);

    StatusCode ret = StatusCode::sOk;

    do {
        auto it = m_backups.find(m_latest_backupname);
        if(it == m_backups.end()){
            ret = StatusCode::sBackupNotExist; 
            break; 
        }

        BackupStatus cur_backup_status = it->second.backup_status;
        switch(cur_backup_status){
            case BackupStatus::BACKUP_CREATING:
                std::async(std::launch::async, 
                           std::bind(&BackupMds::async_create_backup, this)); 
                break;
            case BackupStatus::BACKUP_DELETING:
                std::async(std::launch::async, 
                           std::bind(&BackupMds::async_delete_backup, this)); 
                break;
            default:
                ret = StatusCode::sSnapUpdateError;
                break;
        }
    } while(0);

    ack->mutable_header()->set_status(ret);
    LOG_INFO << "update  vname:" << vol_name << " sname:" << snap_name << " ok";
    return ret;
}

string BackupMds::spawn_cow_object_name(const snapid_t snap_id, 
                                        const block_t  blk_id)
{
    /*todo: may add pool name*/
    string cow_object_name = m_volume_name;
    cow_object_name.append(FS);
    cow_object_name.append(to_string(snap_id));
    cow_object_name.append(FS);
    cow_object_name.append(to_string(blk_id));
    cow_object_name.append(OBJ_SUFFIX);
    return cow_object_name;
}

void BackupMds::split_cow_object_name(const string& raw, 
                                        string&   vol_name,
                                        snapid_t& snap_id, 
                                        block_t&  blk_id)
{
    /*todo */
    return;
}

StatusCode BackupMds::diff_snapshot(const DiffReq* req, DiffAck* ack)
{
    string  vname = req->vol_name();
    string  first_snap = req->first_snap_name();
    string  last_snap  = req->laste_snap_name();

    LOG_INFO << "diff snapshot vname:" << vname 
             << " first_snap:" << first_snap << " last_snap:"  << last_snap;

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t first_snapid = get_snapshot_id(first_snap);
    snapid_t last_snapid  = get_snapshot_id(last_snap); 
    assert(first_snapid != -1 && m_latest_snapid != -1);

    auto first_snap_it = m_cow_block_map.find(first_snapid);
    auto last_snap_it  = m_cow_block_map.find(last_snapid);
    assert(first_snap_it != m_cow_block_map.end() && 
           last_snap_it  != m_cow_block_map.end());
    
    for(auto cur_snap_it = first_snap_it ; cur_snap_it != last_snap_it; 
         cur_snap_it++){
        auto next_snap_it = std::next(cur_snap_it, 1);
        map<block_t, cow_object_t>& cur_block_map  = cur_snap_it->second; 
        map<block_t, cow_object_t>& next_block_map = next_snap_it->second; 

        DiffBlocks* diffblocks = ack->add_diff_blocks();
        diffblocks->set_vol_name(vname);
        diffblocks->set_snap_name(get_snapshot_name(cur_snap_it->first));
        for(auto cur_blk_it : cur_block_map){
            auto next_blk_it = next_block_map.find(cur_blk_it.first); 
            if(next_blk_it == next_block_map.end()){
                /*block not apear in next snapshot*/
                diffblocks->add_diff_block_no(cur_blk_it.first); 
            } else {
                /*block in next snapshot, but cow after next snapshot*/
                if(cur_blk_it.second.compare(next_blk_it->second) != 0){
                    diffblocks->add_diff_block_no(cur_blk_it.first); 
                } 
            }
        }
    }
    
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "diff snapshot" << " vname:" << vname 
             << " first_snap:" << first_snap << " last_snap:"  << last_snap
             << " ok";
    return StatusCode::sOk;
}

StatusCode BackupMds::read_snapshot(const ReadReq* req, ReadAck* ack)
{
    string vol_name  = req->vol_name();
    string snap_name = req->snap_name();
    off_t off  = req->off();
    size_t len = req->len();

    LOG_INFO << "read snapshot" << " vname:" << vol_name
             << " sname:" << snap_name << " off:"  << off << " len:"  << len;
    
    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    auto snap_it = m_cow_block_map.find(snap_id);
    map<block_t, cow_object_t>& block_map = snap_it->second;
    
    for(auto blk_it : block_map){
        block_t blk_no  = blk_it.first;
        off_t   blk_start = blk_no * COW_BLOCK_SIZE;
        off_t   blk_end   = (blk_no+1) * COW_BLOCK_SIZE;
        if(blk_start > off + len || blk_end < off){
            continue; 
        }
        ReadBlock* rblock = ack->add_read_blocks();
        rblock->set_blk_no(blk_no);
        rblock->set_blk_object(blk_it.second);
    }

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "read snapshot" << " vname:" << vol_name
             << " sname:" << snap_name << " off:"  << off << " len:"  << len
             << " ok";
    return StatusCode::sOk; 
}

int BackupMds::recover()
{
    IndexStore::SimpleIteratorPtr it = m_index_store->db_iterator();
    
    /*recover latest snapshot id*/
    string prefix = BACKUPSHOT_ID_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
          it->next()){
        string value = it->value();
        m_latest_snapid = atol(value.c_str());
    } 
    
    /*recover latest snapshot name*/
    prefix = BACKUPSHOT_NAME_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
          it->next()){
        string value = it->value();
        m_latest_snapname = value;
    } 
    
    /*recover snapshot attr map*/
    prefix = BACKUPSHOT_MAP_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
            it->next()){
        string key = it->key();
        string value = it->value();
        
        string snap_name;
        DbUtil::split_attr_map_key(key, snap_name);

        snap_attr_t snap_attr;
        DbUtil::split_attr_map_val(value, snap_attr);

        m_snapshots.insert({snap_name, snap_attr});
    } 
  
    /*recover snapshot cow block map*/
    for(auto snap : m_snapshots){
        snapid_t snap_id = snap.second.snap_id;
        map<block_t, cow_object_t> cow_block_map;
        
        prefix = BACKUPSHOT_COWBLOCK_PREFIX;
        prefix.append(FS);
        prefix.append(to_string(snap_id));
        prefix.append(FS);
        it->seek_to_first(prefix);                                                  

        for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
                it->next()){
            snapid_t snap_id;
            block_t  blk_id;
            DbUtil::split_cow_block_map_key(it->key(), snap_id, blk_id);
            cow_block_map.insert(pair<block_t, cow_object_t>(blk_id, it->value()));
        } 
        
        m_cow_block_map.insert(pair<snapid_t, map<block_t, cow_object_t>> \
                               (snap_id, cow_block_map));
    }
    
    /*recove cow object reference*/
    prefix = BACKUPSHOT_COWOBJECT_PREFIX;
    it->seek_to_first(prefix);                                                  
    for(; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str()); 
            it->next()){
        cow_object_t cow_object;
        DbUtil::split_cow_object_map_key(it->key(), cow_object);
        cow_object_ref_t cow_object_ref;
        DbUtil::split_cow_object_map_val(it->value(), cow_object_ref);
        m_cow_object_map.insert(pair<cow_object_t, cow_object_ref_t> \
                                (cow_object, cow_object_ref));
    } 
    
    trace();

    LOG_INFO << "drserver recover snapshot meta data ok";
    return 0;
}

void BackupMds::trace()
{
    LOG_INFO << "\t latest backup id:" << m_latest_backupid;
    LOG_INFO << "\t latest backup name:" << m_latest_backupname;
    LOG_INFO << "\t current snapshot";
    for(auto it : m_backups){
        LOG_INFO << "\t\t backup_name:" << it.first 
                 << " backup_id:" << it.second.backup_id
                 << " backup_status:" << it.second.backup_status;
    }

    LOG_INFO << "\t backup block map";
    for(auto it : m_bakcup_block_map){
        LOG_INFO << "\t\t backup_id:" << it.first;
        map<block_t, bakcup_object_t>& blk_map = it.second;
        for(auto blk_it : blk_map){
            LOG_INFO << "\t\t\t blk_no:" << blk_it.first 
                     << " backup_obj:"   << blk_it.second;
        }
    }

    LOG_INFO << "\t backup object map";
    for(auto it : m_backup_object_map){
        LOG_INFO << "\t\t backup_obj:" << it.first;
        backup_object_ref_t& backup_obj_ref = it.second;
        for(auto ref_it : backup_obj_ref){
            LOG_INFO << "\t\t\t\t backupid:" << ref_it;
        }
    }
}
