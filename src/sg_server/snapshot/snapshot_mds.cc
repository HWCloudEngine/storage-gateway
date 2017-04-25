/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    snapshot.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot interface
* 
*************************************************/
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include "snapshot_util.h"
#include "snapshot_mds.h"

using huawei::proto::DiffBlocks;
using huawei::proto::inner::UpdateEvent;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;

SnapshotMds::SnapshotMds(const string& vol_name, const size_t& vol_size) {
    m_volume_name = vol_name;
    m_volume_size = vol_size;

    m_latest_snapid = 0;

    m_snapshots.clear();
    m_cow_block_map.clear();
    m_cow_object_map.clear();

    m_block_store = new CephBlockStore();

    /*todo: read from configure file*/
    string db_path = DB_DIR + vol_name + "/snapshot";
    if (access(db_path.c_str(), F_OK)) {
        char cmd[256] = "";
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", db_path.c_str());
        int ret = system(cmd);
        assert(ret != -1);
    }
    m_index_store = IndexStore::create("rocksdb", db_path);
    m_index_store->db_open();
}

SnapshotMds::~SnapshotMds() {
    if (m_index_store) {
        delete m_index_store;
    }
    if (m_block_store) {
        delete m_block_store;
    }
    m_cow_object_map.clear();
    m_cow_block_map.clear();
    m_snapshots.clear();
}

snapid_t SnapshotMds::spawn_snapshot_id() {
    lock_guard<std::mutex> lock(m_mutex);
    /*todo: how to maintain and recycle snapshot id*/
    snapid_t snap_id = m_latest_snapid;
    m_latest_snapid++;
    return snap_id;
}

snapid_t SnapshotMds::get_snapshot_id(string snap_name) {
    auto it = m_snapshots.find(snap_name);
    if (it != m_snapshots.end()) {
        return it->second.snap_id;
    }
    return -1;
}

string SnapshotMds::get_snapshot_name(snapid_t snap_id) {
    for (auto it : m_snapshots) {
        if (it.second.snap_id == snap_id) {
            return it.first;
        }
    }
    return "";
}

string SnapshotMds::get_latest_snap_name() {
    auto rit = m_cow_block_map.rbegin();
    if (rit == m_cow_block_map.rend()) {
        return "";
    }
    snapid_t snap_id = rit->first;
    if (snap_id == -1) {
        return "";
    }
    string snap_name = get_snapshot_name(snap_id);
    return snap_name;
}

StatusCode SnapshotMds::sync(const SyncReq* req, SyncAck* ack) {
    string vol_name = req->vol_name();
    LOG_INFO << "sync" << " vname:" << vol_name;
    lock_guard<std::mutex> lock(m_mutex);
    string latest_snap_name = get_latest_snap_name();
    ack->set_latest_snap_name(latest_snap_name);
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "sync" << " vname:" << vol_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::create_snapshot(const CreateReq* req, CreateAck* ack) {
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "create snapshot vname:" << vol_name << " sname:" << snap_name;

    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_snapshots.find(snap_name);
    if (it != m_snapshots.end()) {
        ack->mutable_header()->set_status(StatusCode::sSnapAlreadyExist);
        LOG_INFO << "create snapshot vname:" << vol_name <<" sname:"
                 << snap_name <<" failed already exist";
        return StatusCode::sOk;
    }

    snap_attr_t cur_snap_attr;
    cur_snap_attr.replication_uuid = req->header().replication_uuid();
    cur_snap_attr.checkpoint_uuid  = req->header().checkpoint_uuid();
    cur_snap_attr.volume_uuid = vol_name;
    cur_snap_attr.snap_type = (SnapType)req->header().snap_type();
    cur_snap_attr.snap_name = snap_name;
    cur_snap_attr.snap_status = SnapStatus::SNAP_CREATING;

    /*in db update snapshot status*/
    string pkey = DbUtil::spawn_attr_map_key(snap_name);
    string pval = DbUtil::spawn_attr_map_val(cur_snap_attr);
    m_index_store->db_put(pkey, pval);

    /*in memroy update snapshot status*/
    m_snapshots.insert({snap_name, cur_snap_attr});

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "create snapshot vname:" << vol_name
             << " sname:" << snap_name <<" ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::list_snapshot(const ListReq* req, ListAck* ack) {
    string vol_name = req->vol_name();
    LOG_INFO << "list snapshot vname:" << vol_name;
    lock_guard<std::mutex> lock(m_mutex);
    for (auto it : m_snapshots) {
        ack->add_snap_name(it.first);
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "list snapshot vname:" << vol_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::query_snapshot(const QueryReq* req, QueryAck* ack) {
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "query snapshot vname:" << vol_name << " snap:" << snap_name;

    lock_guard<std::mutex> lock(m_mutex);
    StatusCode ret = StatusCode::sOk;
    do {
        auto it = m_snapshots.find(snap_name);
        if (it == m_snapshots.end()) {
            ack->set_snap_status(SnapStatus::SNAP_DELETED);
            break;
        }
        ack->set_snap_status(it->second.snap_status);
    }while(0);

    ack->mutable_header()->set_status(ret);
    LOG_INFO << "query snapshot vname:" << vol_name << " snap:" << snap_name
             << " snap_status:" << ack->snap_status() << " ok";
    return ret;
}

StatusCode SnapshotMds::delete_snapshot(const DeleteReq* req, DeleteAck* ack) {
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "delete snapshot vname:" << vol_name << " sname:" << snap_name;

    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_snapshots.find(snap_name);
    if (it == m_snapshots.end()) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "delete snapshot vname:" << vol_name
                 << " sname:" << snap_name << " failed no exist";
        return StatusCode::sOk;
    }
    SnapStatus cur_snap_status = SnapStatus::SNAP_DELETING;
    /*in memroy update snapshot status*/
    it->second.snap_status = cur_snap_status;

    /*in db update snapshot status*/
    string pkey = DbUtil::spawn_attr_map_key(snap_name);
    string pval = DbUtil::spawn_attr_map_val(it->second);
    m_index_store->db_put(pkey, pval);

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "delete snapshot vname:" << vol_name
             << " sname:" << snap_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::rollback_snapshot(const RollbackReq* req,
                                          RollbackAck* ack) {
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    LOG_INFO << "rollback snapshot vname:" << vol_name
             << " snap_name:" << snap_name;

    lock_guard<std::mutex> lock(m_mutex);
    string cur_snap_name = mapping_snap_name(req->header(), snap_name);
    if (cur_snap_name.empty()) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
                 << " snap_name:" << snap_name << "not exist";
        return StatusCode::sOk;
    }

    auto it = m_snapshots.find(cur_snap_name);
    if (it == m_snapshots.end()) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
                 << " snap_name:" << snap_name << "not exist";
        return StatusCode::sOk;
    }

    snapid_t snap_id = it->second.snap_id;
    auto snap_it = m_cow_block_map.find(snap_id);
    if (snap_it == m_cow_block_map.end()) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
                 << " snap_id:" << snap_id << " not exist";
        return StatusCode::sOk;
    }

    map<block_t, cow_object_t>& block_map = snap_it->second;
    for (auto blk_it : block_map) {
        RollBlock* roll_blk = ack->add_roll_blocks();
        roll_blk->set_blk_no(blk_it.first);
        roll_blk->set_blk_object(blk_it.second);
    }

    SnapStatus cur_snap_status = SnapStatus::SNAP_ROLLBACKING;
    m_snapshots[cur_snap_name].snap_status = cur_snap_status;

    string pkey = DbUtil::spawn_attr_map_key(cur_snap_name);
    string pval = DbUtil::spawn_attr_map_val(m_snapshots[cur_snap_name]);
    m_index_store->db_put(pkey, pval);

    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "rollback snapshot vname:" << vol_name
             << " snap_name:" << snap_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::create_event_update_status(string snap_name) {
    auto it = m_snapshots.find(snap_name);
    if (it == m_snapshots.end()) {
        return StatusCode::sSnapNotExist;
    }

    if (it->second.snap_status != SnapStatus::SNAP_CREATING) {
        return StatusCode::sSnapAlreadyExist;
    }
    /*generate snapshot id*/
    snapid_t snap_id  = spawn_snapshot_id();

    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey = DbUtil::spawn_latest_id_key();
    string pval = to_string(snap_id+1);
    transaction->put(pkey, pval);
    pkey = DbUtil::spawn_latest_name_key();
    pval = snap_name;
    transaction->put(pkey, pval);
    pkey = DbUtil::spawn_attr_map_key(snap_name);
    snap_attr_t snap_attr = it->second;
    snap_attr.snap_id = snap_id;
    snap_attr.snap_status = SnapStatus::SNAP_CREATED;
    pval = DbUtil::spawn_attr_map_val(snap_attr);
    transaction->put(pkey, pval);
    int ret = m_index_store->submit_transaction(transaction);
    if (ret) {
        return StatusCode::sSnapMetaPersistError;
    }

    /*update snapshot attr*/
    it->second.snap_id = snap_id;
    it->second.snap_status = SnapStatus::SNAP_CREATED;

    /*prepare cow block map*/
    map<block_t, cow_object_t> block_map;
    m_cow_block_map.insert({snap_id, block_map});
    LOG_INFO << "update sname:" << snap_name << " create event ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::delete_event_update_status(string snap_name) {
    lock_guard<std::mutex> lock(m_mutex);
    auto it = m_snapshots.find(snap_name);
    if (it == m_snapshots.end()) {
        return StatusCode::sSnapNotExist;
    }

    /*all snapshot status support delete*/
    if (it->second.snap_status == SnapStatus::SNAP_CREATING) {
        m_snapshots.erase(snap_name);
        string pkey = DbUtil::spawn_attr_map_key(snap_name);
        m_index_store->db_del(pkey);
        return StatusCode::sOk;
    }

    if (it->second.snap_status == SnapStatus::SNAP_DELETED) {
        return StatusCode::sOk;
    }

    snapid_t snap_id = it->second.snap_id;
    auto snap_it = m_cow_block_map.find(snap_id);
    if (snap_it == m_cow_block_map.end()) {
        return StatusCode::sSnapNotExist;
    }

    map<block_t, cow_object_t>& block_map = snap_it->second;
    /*-----transction begin-----*/
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    string pkey;
    string pval;
    /*delete cow block in the snapshot*/
    for (auto blk_it : block_map) {
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
        if (obj_ref.empty()) {
            transaction->del(pkey);
        }
    }
    pkey = DbUtil::spawn_attr_map_key(snap_name);
    transaction->del(pkey);
    int ret = m_index_store->submit_transaction(transaction);
    if (ret) {
        return StatusCode::sSnapMetaPersistError;
    }
    /*-----transction end-----*/

    for (auto blk_it : block_map) {
        cow_object_t cow_obj = blk_it.second;

        /*in memory update cow object reference*/
        cow_object_ref_t& cow_obj_ref = m_cow_object_map[cow_obj];
        cow_obj_ref.erase(snap_id);

        if (cow_obj_ref.empty()) {
            /*in mem delete cow object key*/
            m_cow_object_map.erase(cow_obj);
            /*block store reclaim the cow object*/
            m_block_store->remove(cow_obj);
        }
    }
    block_map.clear();

    m_cow_block_map.erase(snap_id);

    m_snapshots.erase(snap_name);
    pkey = DbUtil::spawn_attr_map_key(snap_name);
    m_index_store->db_del(pkey);

    trace();

    LOG_INFO << "update sname:" << snap_name << " delete event ok";
    return StatusCode::sOk;
}

string SnapshotMds::mapping_snap_name(const SnapReqHead& shead,
                                      const string& sname) {
    if (shead.replication_uuid().empty() &&
        shead.checkpoint_uuid().empty() &&
        shead.snap_type() == SnapType::SNAP_LOCAL) {
        /*local snapshot */
        return sname;
    }

    /*remote snapshot, only check checkpoint_uuid same*/
    const string& checkpoint = shead.checkpoint_uuid();
    for (auto it : m_snapshots) {
        if (it.second.checkpoint_uuid.compare(checkpoint) == 0) {
            return it.second.snap_name;
        }
    }

    return string();
}

StatusCode SnapshotMds::update(const UpdateReq* req, UpdateAck* ack) {
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    UpdateEvent snap_event = req->snap_event();

    LOG_INFO << "update vname:" << vol_name << " sname:" << snap_name
             << " event:" << snap_event;
    StatusCode ret = StatusCode::sOk;
    string cur_snap_name = mapping_snap_name(req->header(), snap_name);

    do {
        if (cur_snap_name.empty()) {
            ret = StatusCode::sSnapNotExist;
            break;
        }
        auto it = m_snapshots.find(cur_snap_name);
        if (it == m_snapshots.end()) {
            ret = StatusCode::sSnapNotExist;
            break;
        }
        switch (snap_event) {
            case UpdateEvent::CREATE_EVENT:
                create_event_update_status(cur_snap_name);
                break;
            case UpdateEvent::DELETE_EVENT:
                delete_event_update_status(cur_snap_name);
                break;
            case UpdateEvent::ROLLBACK_EVENT:
                // do nothing
                break;
            default:
                ret = StatusCode::sSnapUpdateError;
                break;
        }
    }while(0);
    ack->mutable_header()->set_status(ret);
    /*get the latest snapshot in system*/
    string latest_snap_name = get_latest_snap_name();
    ack->set_latest_snap_name(latest_snap_name);
    LOG_INFO << "update vname:" << vol_name << " sname:" << snap_name
             << " event:" << snap_event << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::cow_op(const CowReq* req, CowAck* ack) {
    string vname = req->vol_name();
    string snap_name = req->snap_name();
    block_t blk_id = req->blk_no();

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    LOG_INFO << "cow_op vname:" << vname << " snap_name:" << snap_name
             << " snap_id:" << snap_id << " blk_id:"  << blk_id;

    auto snap_it = m_cow_block_map.find(snap_id);
    map<block_t, cow_object_t>& block_map = snap_it->second;
    auto blk_it = block_map.find(blk_id);
    if (blk_it != block_map.end()) {
        /*block already cow*/
        LOG_INFO << "cow_op COW_NO";
        ack->mutable_header()->set_status(StatusCode::sOk);
        ack->set_op(COW_NO);
        return StatusCode::sOk;
    }

    /*create cow object*/
    cow_object_t cow_object = spawn_cow_object_name(snap_id, blk_id);
    /*block store create cow object*/
    m_block_store->create(cow_object);
    ack->mutable_header()->set_status(StatusCode::sOk);
    ack->set_op(COW_YES);
    ack->set_cow_blk_object(cow_object);
    LOG_INFO << "cow_op COW_YES " << " cow_object:" << cow_object;
    return StatusCode::sOk;
}

StatusCode SnapshotMds::cow_update(const CowUpdateReq* req, CowUpdateAck* ack) {
    string vname = req->vol_name();
    string snap_name = req->snap_name();
    block_t blk_no = req->blk_no();
    string cow_obj = req->cow_blk_object();

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    LOG_INFO << "cow_update vname:" << vname << " snap_name:" << snap_name
             << " snap_id:" << snap_id << " blk_id:"  << blk_no
             << " cow_obj:" << cow_obj;

    /*----transaction begin-------*/
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    /*in db update cow block*/
    string pkey = DbUtil::spawn_cow_block_map_key(snap_id, blk_no);
    transaction->put(pkey, cow_obj);
    /*in db update cow object */
    pkey = DbUtil::spawn_cow_object_map_key(cow_obj);
    cow_object_ref_t cow_obj_ref;
    cow_obj_ref.insert(snap_id);
    string pval = DbUtil::spawn_cow_object_map_val(cow_obj_ref);
    transaction->put(pkey, pval);
    int ret = m_index_store->submit_transaction(transaction);
    if (ret) {
        return StatusCode::sSnapMetaPersistError;
    }

    /*in mem update cow_block_map*/
    auto snap_it = m_cow_block_map.find(snap_id);
    assert(snap_it != m_cow_block_map.end());
    map<block_t, cow_object_t>& block_map = snap_it->second;
    block_map.insert({blk_no, cow_obj});

    /*in mem update cow object*/
    auto obj_it = m_cow_object_map.find(cow_obj);
    assert(obj_it == m_cow_object_map.end());
    m_cow_object_map.insert({cow_obj, cow_obj_ref});

    /*todo :travel forward to update other snapshot in cow_block_map
     * should optimize by backward*/
    auto travel_it = m_cow_block_map.begin();
    for (; travel_it != m_cow_block_map.end() && travel_it->first < snap_id;
          travel_it++) {
        map<block_t, cow_object_t>& block_map = travel_it->second;
        auto blk_it = block_map.find(blk_no);
        if (blk_it != block_map.end()) {
            LOG_INFO << "cow_update snapshot: " << travel_it->first
                     << " has block:" << blk_no << " break";
            continue;
        }

        LOG_INFO << "cow_update snapshot: " << travel_it->first
                 << " has no block:" << blk_no;
        IndexStore::Transaction transaction = m_index_store->fetch_transaction();
        /*in db update cow block*/
        string pkey = DbUtil::spawn_cow_block_map_key(travel_it->first, blk_no);
        transaction->put(pkey, cow_obj);
        /*in db update cow object(move up)*/
        pkey = DbUtil::spawn_cow_object_map_key(cow_obj);
        string pval = m_index_store->db_get(pkey);
        cow_object_ref_t obj_ref;
        DbUtil::split_cow_object_map_val(pval, obj_ref);
        obj_ref.insert(travel_it->first);
        pval = DbUtil::spawn_cow_object_map_val(obj_ref);
        transaction->put(pkey, pval);
        ret = m_index_store->submit_transaction(transaction);
        if (ret) {
            LOG_ERROR << "cow_update persist meta failed";
            return StatusCode::sSnapMetaPersistError;
        }

        /*in mem update cow block*/
        block_map.insert({blk_no, cow_obj});
        /*in mem update cow object*/
        auto obj_it = m_cow_object_map.find(cow_obj);
        assert(obj_it != m_cow_object_map.end());
        cow_object_ref_t& cow_obj_ref = obj_it->second;
        cow_obj_ref.insert(travel_it->first);
    }

    trace();
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "cow_update vname:" << vname << " snap_name:" << snap_name
             << " snap_id:" << snap_id << " blk_id:" << blk_no
             << " cow_obj:" << cow_obj << " ok";
    return StatusCode::sOk;
}

string SnapshotMds::spawn_cow_object_name(const snapid_t snap_id,
                                          const block_t  blk_id) {
    /*todo: may add pool name*/
    string cow_object_name = m_volume_name;
    cow_object_name.append(FS);
    cow_object_name.append(to_string(snap_id));
    cow_object_name.append(FS);
    cow_object_name.append(to_string(blk_id));
    cow_object_name.append(OBJ_SUFFIX);
    return cow_object_name;
}

StatusCode SnapshotMds::diff_snapshot(const DiffReq* req, DiffAck* ack) {
    string vname = req->vol_name();
    string first_snap = req->first_snap_name();
    string last_snap  = req->last_snap_name();

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
    for (auto cur_snap_it = first_snap_it ; cur_snap_it != last_snap_it;
         cur_snap_it++) {
        auto next_snap_it = std::next(cur_snap_it, 1);
        map<block_t, cow_object_t>& cur_block_map  = cur_snap_it->second;
        map<block_t, cow_object_t>& next_block_map = next_snap_it->second;

        DiffBlocks* diffblocks = ack->add_diff_blocks();
        diffblocks->set_vol_name(vname);
        diffblocks->set_snap_name(get_snapshot_name(cur_snap_it->first));
        for (auto cur_blk_it : cur_block_map) {
            auto next_blk_it = next_block_map.find(cur_blk_it.first);
            if (next_blk_it == next_block_map.end()) {
                /*block not apear in next snapshot*/
                diffblocks->add_diff_block_no(cur_blk_it.first);
            } else {
                /*block in next snapshot, but cow after next snapshot*/
                if (cur_blk_it.second.compare(next_blk_it->second) != 0) {
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

StatusCode SnapshotMds::read_snapshot(const ReadReq* req, ReadAck* ack) {
    string vol_name = req->vol_name();
    string snap_name = req->snap_name();
    off_t off = req->off();
    size_t len = req->len();

    LOG_INFO << "read snapshot" << " vname:" << vol_name
             << " sname:" << snap_name << " off:"  << off << " len:"  << len;

    lock_guard<std::mutex> lock(m_mutex);
    snapid_t snap_id = get_snapshot_id(snap_name);
    assert(snap_id != -1);
    auto snap_it = m_cow_block_map.find(snap_id);
    map<block_t, cow_object_t>& block_map = snap_it->second;

    for (auto blk_it : block_map) {
        block_t blk_no = blk_it.first;
        off_t blk_start = blk_no * COW_BLOCK_SIZE;
        off_t blk_end = (blk_no+1) * COW_BLOCK_SIZE;
        if (blk_start > off + len || blk_end < off) {
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

int SnapshotMds::recover() {
    IndexStore::SimpleIteratorPtr it = m_index_store->db_iterator();

    /*recover latest snapshot id*/
    string prefix = SNAPSHOT_ID_PREFIX;
    it->seek_to_first(prefix);
    for (; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str());
          it->next()) {
        string value = it->value();
        m_latest_snapid = atol(value.c_str());
    }

    /*recover snapshot attr map*/
    prefix = SNAPSHOT_MAP_PREFIX;
    it->seek_to_first(prefix);
    for (; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str());
           it->next()) {
        string key = it->key();
        string value = it->value();
        string snap_name;
        DbUtil::split_attr_map_key(key, snap_name);
        snap_attr_t snap_attr;
        DbUtil::split_attr_map_val(value, snap_attr);
        m_snapshots.insert({snap_name, snap_attr});
    }

    /*recover snapshot cow block map*/
    for (auto snap : m_snapshots) {
        snapid_t snap_id = snap.second.snap_id;
        map<block_t, cow_object_t> cow_block_map;
        prefix = SNAPSHOT_COWBLOCK_PREFIX;
        prefix.append(FS);
        prefix.append(to_string(snap_id));
        prefix.append(FS);
        it->seek_to_first(prefix);
        for (; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str());
                it->next()) {
            snapid_t snap_id;
            block_t  blk_id;
            DbUtil::split_cow_block_map_key(it->key(), snap_id, blk_id);
            cow_block_map.insert({blk_id, it->value()});
        }

        m_cow_block_map.insert(pair<snapid_t, map<block_t, cow_object_t>> \
                               (snap_id, cow_block_map));
    }

    /*recove cow object reference*/
    prefix = SNAPSHOT_COWOBJECT_PREFIX;
    it->seek_to_first(prefix);
    for (; it->valid()&& !it->key().compare(0, prefix.size(), prefix.c_str());
            it->next()) {
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

void SnapshotMds::trace() {
    LOG_INFO << "\t latest snapid:" << m_latest_snapid;
    LOG_INFO << "\t current snapshot";
    for (auto it : m_snapshots) {
        LOG_INFO << "\t\t snap_name:" << it.first
                 << " snap_id:" << it.second.snap_id
                 << " snap_status:" << it.second.snap_status;
    }

    LOG_INFO << "\t cow block map";
    for (auto it : m_cow_block_map) {
        LOG_INFO << "\t\t snap_id:" << it.first;
        map<block_t, cow_object_t>& blk_map = it.second;
        for (auto blk_it : blk_map) {
            LOG_INFO << "\t\t\t blk_no:" << blk_it.first
                     << " cow_obj:" << blk_it.second;
        }
    }
    LOG_INFO << "\t cow object map";
    for (auto it : m_cow_object_map) {
        LOG_INFO << "\t\t cow_obj:" << it.first;
        cow_object_ref_t& cow_obj_ref = it.second;
        for (auto ref_it : cow_obj_ref) {
            LOG_INFO << "\t\t\t\t snapid:" << ref_it;
        }
    }
}
