#include <cstdlib>
#include <assert.h>
#include "snapshot_mds.h"

string DbUtil::spawn_key(const string& prefix, const string& value)
{
    string key = prefix;
    key.append(FS);
    key.append(value);
    return key;
}

void DbUtil::split_key(const string& raw_key, string& prefix, string& key)
{
    size_t pos = raw_key.find(FS, 0);
    prefix = raw_key.substr(0, pos);
    key = raw_key.substr(pos+1, raw_key.length());
}

string DbUtil::spawn_latest_id_key()
{
    return spawn_key(SNAPSHOT_ID_PREFIX, "");
}

string DbUtil::spawn_latest_name_key()
{
    return spawn_key(SNAPSHOT_NAME_PREFIX, "");
}

string DbUtil::spawn_ids_map_key(const string& snap_name)
{
    return spawn_key(SNAPSHOT_MAP_PREFIX, snap_name);
}

void DbUtil::split_ids_map_key(const string& raw_key, string& snap_name)
{
    string prefix;
    split_key(raw_key, prefix, snap_name); 
}

string DbUtil::spawn_status_map_key(const string& snap_name)
{
    return spawn_key(SNAPSHOT_STATUS_PREFIX, snap_name);
}

void DbUtil::split_status_map_key(const string& raw_key, 
                                  string& snap_name)
{
    string prefix;
    split_key(raw_key, prefix, snap_name);
}

string DbUtil::spawn_cow_block_map_key(const snapid_t& snap_id,
                                       const block_t& block_id)
{
   string key = to_string(snap_id);
   key.append(FS);
   key += to_string(block_id);
   return spawn_key(SNAPSHOT_COWBLOCK_PREFIX, key);
}

void DbUtil::split_cow_block_map_key(const string& raw_key, 
                                     snapid_t& snap_id, 
                                     block_t& block_id)
{
    string prefix;
    string key;
    split_key(raw_key, prefix, key);

    string snap;
    string block;
    split_key(key, snap, block);

    snap_id = atol(snap.c_str());
    block_id = atol(block.c_str());
}

string DbUtil::spawn_cow_object_map_key(const string& obj_name)
{
    return spawn_key(SNAPSHOT_COWOBJECT_PREFIX, obj_name);
}

void DbUtil::split_cow_object_map_key(const string& raw_key, 
                                         string obj_name)
{
   string prefix;
   split_key(raw_key, prefix, obj_name);
}

string DbUtil::spawn_cow_object_map_val(const cow_object_ref_t& obj_ref)
{
    string val;
    for(auto it : obj_ref){
        val.append(to_string(it));
        val.append(FS);
    }
    return val;
}

void DbUtil::split_cow_object_map_val(const string& raw_val, 
                                      cow_object_ref_t& obj_ref)
{
    string raw = raw_val;
    size_t pos = raw.find(FS);
    while(pos != string::npos){
        string   snap_str = raw.substr(0, pos);
        snapid_t snap_id  = atol(snap_str.c_str());
        obj_ref.insert(snap_id);

        raw = raw.substr(pos+1);
        pos = raw.find(FS); 
    } 
}

SnapshotMds::SnapshotMds(string vol_name)
{
    m_volume_name   = vol_name;
    m_latest_snapid = 0;
    m_snapshots_ids.clear();
    m_snapshots_status.clear();
    m_cow_block_map.clear();
    m_cow_object_map.clear();

    m_block_store = new CephBlockStore();

    /*todo: read from configure file*/
    string db_path = DB_DIR + vol_name;
    m_index_store = new RocksDbIndexStore(db_path);
}

SnapshotMds::~SnapshotMds()
{
    if(m_index_store){
        delete m_index_store;
    }
    if(m_block_store){
        delete m_block_store;
    }
    m_cow_object_map.clear();
    m_cow_block_map.clear();
    m_snapshots_status.clear();
    m_snapshots_ids.clear();
}

snapid_t SnapshotMds::spawn_snapshot_id()
{
    lock_guard<std::mutex> lock(m_mutex);
    /*todo: how to maintain and recycle snapshot id*/
    snapid_t snap_id = m_latest_snapid;
    m_latest_snapid++;
    return snap_id;
}

snapid_t SnapshotMds::get_snapshot_id(string snap_name)
{
    auto it = m_snapshots_ids.find(snap_name);
    if(it != m_snapshots_ids.end()){
        return it->second; 
    }
    return -1;
}

string SnapshotMds::get_snapshot_name(snapid_t snap_id)
{
    for(auto it : m_snapshots_ids){
        if(it.second == snap_id)
            return it.first;
    }
    return nullptr;
}

int SnapshotMds::sync(const SyncReq* req, SyncAck* ack)
{
    string vol_name = req->vol_name();
    LOG_INFO << "SnapshotMds sync" << " vname:" << vol_name;

    lock_guard<std::mutex> lock(m_mutex);
    ack->set_ret(SNAPSHOT_OK);
    ack->set_latest_snap_name(m_latest_snapname); 
    LOG_INFO << "SnapshotMds sync" << " vname:" << vol_name << " ok";
    return SNAPSHOT_OK; 
}

int SnapshotMds::create_snapshot(const CreateReq* req, CreateAck* ack)
{
    string vol_name  = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "SnapshotMds create snapshot" << " vname:" << vol_name 
             << " sname:" << snap_name;

    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_snapshots_status.find(snap_name);
    if(it != m_snapshots_status.end()){
        ack->set_ret(SNAPSHOT_OK);
        LOG_INFO << "SnapshotMds create snapshot" <<" vname:" << vol_name 
                 <<" sname:" << snap_name <<" already exist";
        return SNAPSHOT_OK; 
    }
    
    snapshot_status_t cur_snap_status = SNAPSHOT_CREATING;

     /*in db update snapshot status*/
    string pkey = DbUtil::spawn_status_map_key(snap_name);
    string pval = to_string(cur_snap_status);
    m_index_store->db_put(pkey, pval);

    /*in memroy update snapshot status*/
    m_snapshots_status.insert(pair<string, snapshot_status_t>(snap_name, 
                                                              cur_snap_status));
    ack->set_ret(SNAPSHOT_OK);
    LOG_INFO << "SnapshotMds create snapshot" <<" vname:" << vol_name 
             <<" sname:" << snap_name <<" ok";
    return SNAPSHOT_OK; 
}

int SnapshotMds::list_snapshot(const ListReq* req, ListAck* ack)
{
    string vol_name = req->vol_name();
    LOG_INFO << "SnapshotMds list snapshot" << " vname:" << vol_name;
    
    lock_guard<std::mutex> lock(m_mutex);
    for(auto it : m_snapshots_status){
        ack->add_snap_name(it.first);
    }
    ack->set_ret(SNAPSHOT_OK);

    LOG_INFO << "SnapshotMds list snapshot" << " vname:" << vol_name << " ok";
    return SNAPSHOT_OK; 
}

int SnapshotMds::delete_snapshot(const DeleteReq* req, DeleteAck* ack)
{
    string vol_name  = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "SnapshotMds delete snapshot" << " vname:" << vol_name 
             << " sname:" << snap_name;

    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_snapshots_status.find(snap_name);
    if(it == m_snapshots_status.end()){
        ack->set_ret(SNAPSHOT_OK);
        LOG_INFO << "SnapshotMds delete snapshot" <<" vname:" << vol_name 
                 << " sname:" << snap_name << " already exist";
        return SNAPSHOT_OK; 
    }
    
    snapshot_status_t cur_snap_status = SNAPSHOT_DELETING;

     /*in db update snapshot status*/
    string pkey = DbUtil::spawn_status_map_key(snap_name);
    string pval = to_string(cur_snap_status);
    m_index_store->db_put(pkey, pval);

    /*in memroy update snapshot status*/
    it->second = cur_snap_status;

    ack->set_ret(SNAPSHOT_OK);
    LOG_INFO << "SnapshotMds delete snapshot" << " vname:" << vol_name 
             << " sname:" << snap_name << " ok";
    return SNAPSHOT_OK; 
}

int SnapshotMds::rollback_snapshot(const RollbackReq* req, RollbackAck* ack)
{
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "SnapshotMds rollback snapshot" << " vname:" << vol_name
             << " snap_name:" << snap_name;
 
    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id !=  -1);

    auto snap_it = m_cow_block_map.find(snap_id);
    if(snap_it == m_cow_block_map.end()){
        ack->set_ret(SNAPSHOT_OK);
        LOG_INFO << "SnapshotMds rollback snapshot" << " vname:" << vol_name
                 << " snap_id:" << snap_id << " already rollback";
        return SNAPSHOT_OK;
    }

    map<block_t, cow_object_t>& block_map = snap_it->second;
    for(auto blk_it : block_map)
    {
        ::huawei::proto::RollBlock* roll_blk = ack->add_roll_blocks();
        roll_blk->set_blk_no(blk_it.first);
        roll_blk->set_blk_object(blk_it.second);
    } 

    snapshot_status_t cur_snap_status = SNAPSHOT_ROLLBACKING;
    string pkey = DbUtil::spawn_status_map_key(snap_name);
    string pval = to_string(cur_snap_status);
    m_index_store->db_put(pkey, pval);
    m_snapshots_status[snap_name] = cur_snap_status;

    ack->set_ret(SNAPSHOT_OK);
    LOG_INFO << "SnapshotMds rollback snapshot" << " vname:" << vol_name
             << " snap_name:" << snap_name << " ok";
    return SNAPSHOT_OK;
}

int SnapshotMds::created_update_status(string snap_name)
{   
    snapid_t snap_id = spawn_snapshot_id();
    string pkey = DbUtil::spawn_latest_id_key();
    string pval = to_string(snap_id);
    m_index_store->db_put(pkey, pval);

    /*update snapshot id*/
    pkey = DbUtil::spawn_ids_map_key(snap_name);
    pval = to_string(snap_id);
    m_index_store->db_put(pkey, pval);
    m_snapshots_ids.insert(pair<string, snapid_t>(snap_name, snap_id));

    /*prepare cow block map*/
    map<block_t, cow_object_t> block_map;
    m_cow_block_map.insert(pair<snapid_t, map<block_t, cow_object_t>>
            (snap_id, block_map));

    /*update snapshot status*/
    snapshot_status_t cur_snap_status = SNAPSHOT_CREATED;
    pkey = DbUtil::spawn_status_map_key(snap_name);
    pval = to_string(cur_snap_status);
    m_index_store->db_put(pkey, pval);
    auto it = m_snapshots_status.find(snap_name);
    if(it != m_snapshots_status.end()){
        it->second = cur_snap_status; 
    }

    m_latest_snapname = snap_name;
    pkey = DbUtil::spawn_latest_name_key();
    pval = m_latest_snapname;
    m_index_store->db_put(pkey, pval);

    return SNAPSHOT_OK;
}

int SnapshotMds::deleted_update_status(string snap_name)
{
    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    auto snap_it = m_cow_block_map.find(snap_id);
    if(snap_it == m_cow_block_map.end()){
        LOG_INFO << "SnapshotMds delete snapshot"
                 << " snap_name:" << snap_name
                 << " snap_id:" << snap_id
                 << " already delete";
        return SNAPSHOT_NOEXIST;
    }
    map<block_t, cow_object_t>& block_map = snap_it->second;

    /*delete cow block in the snapshot*/
    for(auto blk_it : block_map){
        /*in db delete cow block key*/
        string pkey = DbUtil::spawn_cow_block_map_key(snap_id, blk_it.first);
        m_index_store->db_del(pkey);

        cow_object_t cow_obj = blk_it.second; 

        /*in db read modify write cow object reference*/
        pkey = DbUtil::spawn_cow_object_map_key(cow_obj);
        string pval = m_index_store->db_get(pkey);
        cow_object_ref_t obj_ref;
        DbUtil::split_cow_object_map_val(pval, obj_ref);
        obj_ref.erase(snap_id);
        pval = DbUtil::spawn_cow_object_map_val(obj_ref);
        m_index_store->db_put(pkey, pval);

        /*in memory update cow object reference*/
        cow_object_ref_t& cow_obj_ref = m_cow_object_map[cow_obj];
        cow_obj_ref.erase(snap_id);
        if(cow_obj_ref.empty()){
            /*in db delete cow object key*/
            m_index_store->db_del(pkey);
            /*in mem delete cow object key*/
            m_cow_object_map.erase(cow_obj); 

            /*block store reclaim the cow object*/
            m_block_store->remove(cow_obj);
        }
    }
    block_map.clear();

    /*delete snapshot in cow block map*/
    m_cow_block_map.erase(snap_id);
    
    /*snapshot status*/
    string pkey = DbUtil::spawn_status_map_key(snap_name);
    m_index_store->db_del(pkey);
    m_snapshots_status.erase(snap_name);

    /*snapshot table*/
    for(auto it : m_snapshots_ids){
        if(it.first.compare(snap_name) == 0){
            string pkey = DbUtil::spawn_ids_map_key(it.first);
            m_index_store->db_del(pkey);
            m_snapshots_ids.erase(it.first);
            break;
        }
    }
    
    /*update latest snaphsot*/
    if(m_latest_snapname.compare(snap_name)){
        m_latest_snapname.clear();
    }

    trace();

    LOG_INFO << "SnapshotMds delete snapshot" << " snap_name:" << snap_name
             << " ok";
    return SNAPSHOT_OK;
}

int SnapshotMds::update(const UpdateReq* req, UpdateAck* ack)
{
    string vol_name  = req->vol_name();
    string snap_name = req->snap_name();

    LOG_INFO << "SnapshotMds update" << " vname:" << vol_name 
             << " snap_name:" << snap_name;
    
    auto it = m_snapshots_status.find(snap_name);
    assert(it != m_snapshots_status.end());
    snapshot_status_t cur_snap_status = it->second;

    if(cur_snap_status == SNAPSHOT_CREATING){
        created_update_status(snap_name);
    } else if(cur_snap_status == SNAPSHOT_DELETING ||
              cur_snap_status == SNAPSHOT_ROLLBACKING){
        deleted_update_status(snap_name);
    } else {
        ; 
    } 

    ack->set_ret(SNAPSHOT_OK);
    LOG_INFO << "SnapshotMds update" << " vname:" << vol_name 
             << " snap_name:" << snap_name << " ok";
    return SNAPSHOT_OK;
}

int SnapshotMds::cow_op(const CowReq* req, CowAck* ack)
{
    string vname = req->vol_name();
    string snap_name = req->snap_name();
    block_t blk_id = req->blk_no();

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    LOG_INFO << "SnapshotMds cow_op" << " vname:"   << vname 
             << " snap_name:" << snap_name << " snap_id:" << snap_id
             << " blk_id:"  << blk_id;

    auto snap_it = m_cow_block_map.find(snap_id);
    map<block_t, cow_object_t>& block_map = snap_it->second;
    
    auto blk_it = block_map.find(blk_id);
    if(blk_it != block_map.end()){
        /*block already cow*/
        LOG_INFO << "SnapshotMds cow_op COW_NO";
        ack->set_ret(SNAPSHOT_OK);
        ack->set_op(COW_NO);
        return SNAPSHOT_OK;
    }
    
    /*create cow object*/
    cow_object_t cow_object = spawn_cow_object_name(snap_id, blk_id); 
                             
    /*block store create cow object*/
    m_block_store->create(cow_object);

    ack->set_ret(SNAPSHOT_OK);
    ack->set_op(COW_YES);
    ack->set_cow_blk_object(cow_object);

    LOG_INFO << "SnapshotMds cow_op COW_YES " << " cow_object:" << cow_object;
    return SNAPSHOT_OK;
}

void SnapshotMds::trace()
{
    LOG_INFO << "\t current snapshot";
    for(auto it : m_snapshots_ids){
        LOG_INFO << "\t\t snap_name:" << it.first << " snap_id:" << it.second; 
    }
    LOG_INFO << "\t current snapshot status";
    for(auto it : m_snapshots_status){
        LOG_INFO << "\t\t snap_name:" << it.first << " snap_status:" << it.second; 
    }
    LOG_INFO << "\t cow block map";
    for(auto it : m_cow_block_map){
        LOG_INFO << "\t\t snap_id:" << it.first;
        map<block_t, cow_object_t>& blk_map = it.second;
        for(auto blk_it : blk_map){
            LOG_INFO << "\t\t\t blk_no:" << blk_it.first 
                     << " cow_obj:" << blk_it.second;
        }
    }
    LOG_INFO << "\t cow object map";
    for(auto it : m_cow_object_map){
        LOG_INFO << "\t\t cow_obj:" << it.first;
        cow_object_ref_t& cow_obj_ref = it.second;
        for(auto ref_it : cow_obj_ref){
            LOG_INFO << "\t\t\t\t snapid:" << ref_it;
        }
    }
}

int SnapshotMds::cow_update(const CowUpdateReq* req, CowUpdateAck* ack)
{
    string vname   = req->vol_name();
    string snap_name = req->snap_name();
    block_t blk_no  = req->blk_no();
    string cow_obj = req->cow_blk_object();

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    LOG_INFO << "SnapshotMds cow_update" << " vname:" << vname 
             << " snap_name:" << snap_name << " snap_id:" << snap_id
             << " blk_id:"  << blk_no << " cow_obj:" << cow_obj;

    /*in db update cow block*/
    string pkey = DbUtil::spawn_cow_block_map_key(snap_id, blk_no);
    m_index_store->db_put(pkey, cow_obj);

    /*in mem update cow_block_map*/
    auto snap_it = m_cow_block_map.find(snap_id);
    assert(snap_it != m_cow_block_map.end());
    map<block_t, cow_object_t>& block_map = snap_it->second;
    block_map.insert(pair<block_t, cow_object_t>(blk_no, cow_obj));

    /*in db update cow object */
    pkey = DbUtil::spawn_cow_object_map_key(cow_obj);
    cow_object_ref_t cow_obj_ref;
    cow_obj_ref.insert(snap_id);
    string pval = DbUtil::spawn_cow_object_map_val(cow_obj_ref);
    m_index_store->db_put(pkey, pval);

    /*in mem update cow object*/
    auto obj_it = m_cow_object_map.find(cow_obj); 
    assert(obj_it == m_cow_object_map.end());
    m_cow_object_map.insert(pair<cow_object_t, cow_object_ref_t>
                            (cow_obj, cow_obj_ref));

    /*todo :travel forward to update other snapshot in cow_block_map
     * should optimize by backward*/
    auto travel_it = m_cow_block_map.begin();
    for(; travel_it != m_cow_block_map.end() && travel_it->first < snap_id; 
          travel_it++){
        map<block_t, cow_object_t>& block_map = travel_it->second;
        auto blk_it = block_map.find(blk_no);
        if(blk_it != block_map.end()){
            LOG_INFO << "SnapshotMds cow_update snapshot: " << travel_it->first 
                     << " has block:" << blk_no << " break";
            continue;
        }

        LOG_INFO << "SnapshotMds cow_update snapshot: " << travel_it->first 
                 << " has no block:" << blk_no;
        /*in db update cow block*/
        string pkey = DbUtil::spawn_cow_block_map_key(snap_id, blk_no);
        m_index_store->db_put(pkey, cow_obj);
        /*in mem update cow block*/
        block_map.insert(pair<block_t, cow_object_t>(blk_no, cow_obj));

        /*in db update cow object(move up)*/
        pkey = DbUtil::spawn_cow_object_map_key(cow_obj);
        string pval = m_index_store->db_get(pkey);
        cow_object_ref_t obj_ref;
        DbUtil::split_cow_object_map_val(pval, obj_ref);
        obj_ref.insert(travel_it->first);
        pval = DbUtil::spawn_cow_object_map_val(obj_ref);
        m_index_store->db_put(pkey, pval);

        /*in mem update cow object*/
        auto obj_it = m_cow_object_map.find(cow_obj); 
        assert(obj_it != m_cow_object_map.end());
        cow_object_ref_t& cow_obj_ref = obj_it->second;
        cow_obj_ref.insert(travel_it->first);
    }

    trace();

    ack->set_ret(SNAPSHOT_OK);
    LOG_INFO << "SnapshotMds cow_update" << " vname:" << vname 
             << " snap_name:" << snap_name << " snap_id:" << snap_id
             << " blk_id:"  << blk_no << " cow_obj:" << cow_obj << " ok";
    return SNAPSHOT_OK;
}

string SnapshotMds::spawn_cow_object_name(const snapid_t snap_id, 
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

void SnapshotMds::split_cow_object_name(const string& raw, 
                                        string&   vol_name,
                                        snapid_t& snap_id, 
                                        block_t&  blk_id)
{
    /*todo */
    return;
}

int SnapshotMds::diff_snapshot(const DiffReq* req, DiffAck* ack)
{
    string  vname = req->vol_name();
    string  first_snap = req->first_snap_name();
    string  last_snap  = req->laste_snap_name();

    LOG_INFO << "SnapshotMds diff snapshot" << " vname:" << vname 
             << " first_snap:" << first_snap << " last_snap:"  << last_snap;

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t first_snapid = get_snapshot_id(first_snap);
    snapid_t last_snapid  = get_snapshot_id(last_snap); 
    assert(first_snapid != -1 && m_latest_snapid != -1);

    auto first_snap_it = m_cow_block_map.find(first_snapid);
    auto last_snap_it = m_cow_block_map.find(last_snapid);
    assert(first_snap_it != m_cow_block_map.end() && 
           last_snap_it  != m_cow_block_map.end());
    
    for(auto cur_snap_it = first_snap_it ; cur_snap_it != last_snap_it; 
         cur_snap_it++){
        auto next_snap_it = std::next(cur_snap_it, 1);
        map<block_t, cow_object_t>& cur_block_map  = cur_snap_it->second; 
        map<block_t, cow_object_t>& next_block_map = next_snap_it->second; 

        LOG_INFO << "SnapshotMds diff snapshot 0";
        huawei::proto::DiffBlocks* diffblocks = ack->add_diff_blocks();
        diffblocks->set_vol_name(vname);
        diffblocks->set_snap_name(get_snapshot_name(cur_snap_it->first));
        for(auto cur_blk_it : cur_block_map){
            LOG_INFO << "SnapshotMds diff snapshot 1";
            auto next_blk_it = next_block_map.find(cur_blk_it.first); 
            if(next_blk_it == next_block_map.end()){
                /*block not apear in next snapshot*/
                LOG_INFO << "SnapshotMds diff snapshot 2";
                diffblocks->add_diff_block_no(cur_blk_it.first); 
            } else {
                /*block in next snapshot, but cow after next snapshot*/
                if(cur_blk_it.second.compare(next_blk_it->second) != 0){
                    LOG_INFO << "SnapshotMds diff snapshot 3";
                    diffblocks->add_diff_block_no(cur_blk_it.first); 
                } 
            }
        }
    }
    
    ack->set_ret(SNAPSHOT_OK);
    LOG_INFO << "SnapshotMds diff snapshot" << " vname:" << vname 
             << " first_snap:" << first_snap << " last_snap:"  << last_snap
             << " ok";
    return SNAPSHOT_OK;
}

int SnapshotMds::read_snapshot(const ReadReq* req, ReadAck* ack)
{
    string vol_name  = req->vol_name();
    string snap_name = req->snap_name();
    off_t off  = req->off();
    size_t len = req->len();

    LOG_INFO << "SnapshotMds read snapshot" << " vname:" << vol_name
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
        huawei::proto::ReadBlock* rblock = ack->add_read_blocks();
        rblock->set_blk_no(blk_no);
        rblock->set_blk_object(blk_it.second);
    }

    ack->set_ret(SNAPSHOT_OK);

    LOG_INFO << "SnapshotMds read snapshot" << " vname:" << vol_name
             << " sname:" << snap_name << " off:"  << off << " len:"  << len
             << " ok";

    return SNAPSHOT_OK; 
}

int SnapshotMds::recover()
{
    /*todo*/
    return 0;
}
