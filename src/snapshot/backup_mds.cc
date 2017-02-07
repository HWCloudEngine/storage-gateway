#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <future>
#include "../log/log.h"
#include "snapshot_util.h"
#include "backup_mds.h"

using huawei::proto::BackupStatus;
using huawei::proto::DiffBlocks;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;

BackupMds::BackupMds(const string& vol_name, const size_t& vol_size, AbstractMds* snap_mds)
{
    m_vol_name = vol_name;
    m_vol_size = vol_size;
    /*todo how to get volume size*/

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
    cur_backup_attr.backup_mode = req->backup_option().backup_mode();
    cur_backup_attr.backup_status = BackupStatus::BACKUP_CREATING;

    /*(todo)in db update backup status*/

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
    return StatusCode::sOk;
}

backupid_t BackupMds::spawn_backup_id()
{
    lock_guard<std::mutex> lock(m_mutex);
    /*todo: how to maintain and recycle backup id*/
    backupid_t backup_id = m_latest_backup_id;
    m_latest_backup_id++;
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
        if(it.second.backup_id == backup_id)
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
        chunk_size =((end_pos-cur_pos) > DEFAULT_BACKUP_CHUNK_SIZE) ?  \
                      DEFAULT_BACKUP_CHUNK_SIZE : (end_pos-cur_pos);
        
        /*current backup corespondent snapshot*/
        string m_cur_snap = m_cur_backup + ".snap";
        m_snap_client->ReadSnapshot(m_vol_name, m_cur_snap, 
                                    chunk_buf, chunk_size, cur_pos);
        
        block_t cur_blk_no = (cur_pos / DEFAULT_BACKUP_CHUNK_SIZE);
        backupid_t cur_backup_id = get_backup_id(m_cur_backup);
        backup_object_t cur_blk_obj = spawn_backup_object_name(cur_backup_id, cur_blk_no);
        
        /*store backup data to block store in object*/
        int ret = m_block_store->write(cur_blk_obj, chunk_buf, chunk_size, 0);
        assert(ret == 0);

        /*update backup block map*/
        auto cur_backup_block_map_it = m_backup_block_map.find(cur_backup_id);
        assert(cur_backup_block_map_it != m_backup_block_map.end());
        map<block_t, backup_object_t> &cur_backup_block_map = cur_backup_block_map_it->second;
        cur_backup_block_map.insert({cur_blk_no, cur_blk_obj});
        
        /*update backup block obj ref*/
        backup_object_ref_t cur_blk_obj_ref;
        cur_blk_obj_ref.insert(cur_backup_id);
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
    /*2. read diff data(current snapshot and diff region)*/
    /*3. append backup meta(block, object) and (object, backup_ref)*/
    return StatusCode::sOk;
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
    
    if(it->second.backup_mode == BackupMode::BACKUP_FULL){
    } else if(it->second.backup_mode == BackupMode::BACKUP_INCR){
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
        auto it = m_backups.find(m_cur_backup);
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

StatusCode BackupMds::diff_snapshot(const DiffReq* req, DiffAck* ack)
{
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
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "read snapshot" << " vname:" << vol_name
             << " sname:" << snap_name << " off:"  << off << " len:"  << len
             << " ok";
    return StatusCode::sOk; 
}

int BackupMds::recover()
{
    IndexStore::SimpleIteratorPtr it = m_index_store->db_iterator();
    trace();
    LOG_INFO << "drserver recover snapshot meta data ok";
    return 0;
}

StatusCode BackupMds::cow_op(const CowReq* req, CowAck* ack)
{
    return StatusCode::sOk;
}

StatusCode BackupMds::cow_update(const CowUpdateReq* req, CowUpdateAck* ack)
{
    return StatusCode::sOk;
}
 
void BackupMds::trace()
{
    LOG_INFO << "\t latest backup id:" << m_latest_backup_id;
    LOG_INFO << "\t latest full backup name:" << m_latest_full_backup;
    LOG_INFO << "\t current snapshot";
    for(auto it : m_backups){
        LOG_INFO << "\t\t backup_name:" << it.first 
                 << " backup_id:" << it.second.backup_id
                 << " backup_status:" << it.second.backup_status;
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

    LOG_INFO << "\t backup object map";
    for(auto it : m_backup_object_map){
        LOG_INFO << "\t\t backup_obj:" << it.first;
        backup_object_ref_t& backup_obj_ref = it.second;
        for(auto ref_it : backup_obj_ref){
            LOG_INFO << "\t\t\t\t backupid:" << ref_it;
        }
    }
}
