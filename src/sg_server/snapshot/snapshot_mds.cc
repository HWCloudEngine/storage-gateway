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
#include "common/config_option.h"
#include "snapshot_type.h"
#include "snapshot_mds.h"

using huawei::proto::DiffBlock;
using huawei::proto::DiffBlocks;
using huawei::proto::inner::UpdateEvent;
using huawei::proto::inner::ReadBlock;
using huawei::proto::inner::RollBlock;

SnapshotMds::SnapshotMds(const std::string& vol_name, const size_t& vol_size)
    : m_volume_name(vol_name),m_volume_size(vol_size) {
    m_latest_snapid = 0;
    m_block_store = new CephBlockStore(g_option.ceph_cluster_name,
                                       g_option.ceph_user_name,
                                       g_option.ceph_pool_name);
    assert(m_block_store != nullptr);
    std::string db_path = DB_DIR + vol_name + "/snapshot";
    if (access(db_path.c_str(), F_OK)) {
        char cmd[256] = "";
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", db_path.c_str());
        int ret = system(cmd);
        assert(ret != -1);
    }
    m_index_store = IndexStore::create("rocksdb", db_path);
    assert(m_index_store != nullptr);
    m_index_store->db_open();
}

SnapshotMds::~SnapshotMds() {
    if (m_index_store) {
        delete m_index_store;
    }
    if (m_block_store) {
        delete m_block_store;
    }
}

snap_id_t SnapshotMds::get_prev_snap_id(const snap_id_t current) {
    snap_t cur_snap;
    cur_snap.snap_id = current;
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(cur_snap.key());
    it->prev();
    if (!it->valid()) {
        LOG_ERROR << "current:" << current << " no prev snapshot";
        return -1;
    }
    if (-1 == it->key().find(snap_prefix)) {
        LOG_ERROR << "current:" << current << " no prev snapshot";
        return -1;
    }
    snap_t prev_snap;
    prev_snap.decode(it->value());
    LOG_INFO << "current:" << current << " prev:" << prev_snap.snap_id;
    return prev_snap.snap_id;
}

snap_id_t SnapshotMds::get_next_snap_id(const snap_id_t current) {
    snap_t cur_snap;
    cur_snap.snap_id = current;
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(cur_snap.key());
    it->next();
    if (!it->valid()) {
        LOG_ERROR << "current:" << current << " no next snapshot";
        return -1;
    }
    if (-1 == it->key().find(snap_prefix)) {
        LOG_ERROR << "current:" << current << " no next snapshot";
        return -1;
    }
    snap_t next_snap;
    next_snap.decode(it->value());
    LOG_INFO << "current:" << current << " next:" << next_snap.snap_id;
    return next_snap.snap_id;
}

snap_id_t SnapshotMds::get_snap_id(const std::string& snap_name) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid() && (-1 != it->key().find(snap_prefix)); it->next()) {
        if (-1 != it->value().find(snap_name)) {
            snap_t cur_snap;
            cur_snap.decode(it->value());
            return cur_snap.snap_id;
        }
    }
    return -1;
}

std::string SnapshotMds::get_snap_name(snap_id_t snap_id) {
    snap_t cur_snap;
    cur_snap.snap_id = snap_id;
    std::string val = m_index_store->db_get(cur_snap.key());
    cur_snap.decode(val);
    return cur_snap.snap_name;
}

std::string SnapshotMds::get_latest_snap_name() {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_last(snap_prefix);
    if (!it->valid()) {
        LOG_ERROR << "seek to last failed prefix:" << snap_prefix;
        return "";
    }
    snap_t cur_snap;
    cur_snap.decode(it->value());
    LOG_INFO << " latest snap name:" << cur_snap.snap_name;
    return cur_snap.snap_name;
}

snap_id_t SnapshotMds::get_latest_snap_id() {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_last(snap_prefix);
    if (!it->valid()) {
        LOG_ERROR << "seek to last failed prefix:" << snap_prefix;
        return -1;
    }
    snap_t cur_snap;
    cur_snap.decode(it->value());
    LOG_INFO << " latest snap id:" << cur_snap.snap_id;
    return cur_snap.snap_id;
}

StatusCode SnapshotMds::sync(const SyncReq* req, SyncAck* ack) {
    std::string vol_name = req->vol_name();
    LOG_INFO << "sync" << " vname:" << vol_name;
    std::string latest_snap_name = get_latest_snap_name();
    ack->set_latest_snap_name(latest_snap_name);
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "sync" << " vname:" << vol_name << " ok";
    return StatusCode::sOk;
}

bool SnapshotMds::check_snap_exist(const std::string& snap_name) {
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid()&&(-1 != it->key().find(snap_prefix)); it->next()) {
        if (-1 != it->value().find(snap_name)) {
            return true;
        }
    }
    return false;
}

StatusCode SnapshotMds::create_snapshot(const CreateReq* req, CreateAck* ack) {
    std::string vol_name = req->vol_name();
    std::string snap_name = req->snap_name();
    LOG_INFO << "create snapshot vname:" << vol_name << " sname:" << snap_name;
    if (check_snap_exist(snap_name)) {
        ack->mutable_header()->set_status(StatusCode::sSnapAlreadyExist);
        LOG_INFO << "create snapshot vname:" << vol_name <<" sname:"
                 << snap_name <<" failed already exist";
        return StatusCode::sOk;
    }
    snap_t cur_snap;
    cur_snap.vol_id = vol_name;
    cur_snap.rep_id = req->header().replication_uuid();
    cur_snap.ckp_id = req->header().checkpoint_uuid();
    cur_snap.snap_name = snap_name;
    cur_snap.snap_id = m_latest_snapid++; 
    cur_snap.snap_type = (SnapType)req->header().snap_type();
    cur_snap.snap_status = SnapStatus::SNAP_CREATING;
    m_index_store->db_put(cur_snap.key(), cur_snap.encode());
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "create snapshot vname:" << vol_name
             << " sname:" << snap_name <<" ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::list_snapshot(const ListReq* req, ListAck* ack) {
    std::string vol_name = req->vol_name();
    LOG_INFO << "list snapshot vname:" << vol_name;
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid()&&(-1 != it->key().find(snap_prefix)); it->next()) {
        snap_t cur_snap;
        cur_snap.decode(it->value());
        ack->add_snap_name(cur_snap.snap_name);
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "list snapshot vname:" << vol_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::query_snapshot(const QueryReq* req, QueryAck* ack) {
    std::string vol_name = req->vol_name();
    std::string snap_name = req->snap_name();
    StatusCode ret = StatusCode::sOk;
    LOG_INFO << "query snapshot vname:" << vol_name << " snap:" << snap_name;
    ack->set_snap_status(SnapStatus::SNAP_DELETED);
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid()&&(-1 != it->key().find(snap_prefix)); it->next()) {
        snap_t cur_snap;
        cur_snap.decode(it->value());
        if (cur_snap.snap_name.compare(snap_name) == 0) {
            ack->set_snap_status(cur_snap.snap_status);
        }
    }
    ack->mutable_header()->set_status(ret);
    LOG_INFO << "query snapshot vname:" << vol_name << " snap:" << snap_name
             << " snap_status:" << ack->snap_status() << " ok";
    return ret;
}

StatusCode SnapshotMds::delete_snapshot(const DeleteReq* req, DeleteAck* ack) {
    std::string vol_name = req->vol_name();
    std::string snap_name = req->snap_name();
    LOG_INFO << "delete snapshot vname:" << vol_name << " sname:" << snap_name;
    if (!check_snap_exist(snap_name)) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "delete snapshot vname:" << vol_name
                 << " sname:" << snap_name << " failed no exist";
        return StatusCode::sOk;
    }
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid()&&(-1 != it->key().find(snap_prefix)); it->next()) {
        snap_t cur_snap;
        cur_snap.decode(it->value());
        if (cur_snap.snap_name.compare(snap_name) == 0) {
            cur_snap.snap_status = SnapStatus::SNAP_DELETING;
            m_index_store->db_put(cur_snap.key(), cur_snap.encode());
        }
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "delete snapshot vname:" << vol_name
             << " sname:" << snap_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::rollback_snapshot(const RollbackReq* req, RollbackAck* ack) {
    std::string vol_name = req->vol_name();
    std::string snap_name = req->snap_name();
    LOG_INFO << "rollback snapshot vname:" << vol_name << " sname:" << snap_name;
    std::string cur_snap_name = mapping_snap_name(req->header(), snap_name);
    if (cur_snap_name.empty()) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
                 << " snap_name:" << snap_name << "not exist";
        return StatusCode::sOk;
    }
    if (!check_snap_exist(cur_snap_name)) {
        ack->mutable_header()->set_status(StatusCode::sSnapNotExist);
        LOG_INFO << "rollback snapshot vname:" << vol_name
                 << " sname:" << snap_name << "not exist";
        return StatusCode::sOk;
    }
    snap_id_t cur_snap_id = get_snap_id(cur_snap_name);
    cow_block_t cur_pivot_block;
    cur_pivot_block.snap_id = cur_snap_id;
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(cur_pivot_block.prefix());
    for (; it->valid() && (-1 != it->key().find(cur_pivot_block.prefix())); it->next()) {
        cow_block_t cur_block;
        cur_block.decode(it->value());
        RollBlock* roll_blk = ack->add_roll_blocks();
        roll_blk->set_blk_no(cur_block.block_id);
        roll_blk->set_blk_zero(cur_block.block_zero);
        roll_blk->set_blk_url(cur_block.block_url);
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "rollback snapshot vname:" << vol_name
             << " snap_name:" << snap_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::create_event_update_status(const std::string& snap_name) {
    if (!check_snap_exist(snap_name)) {
        LOG_ERROR << "snap:" << snap_name << "no exist";
        return StatusCode::sSnapNotExist;
    }
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid() && (-1 != it->key().find(snap_prefix)); it->next()) {
        snap_t current;
        current.decode(it->value());
        if (current.snap_name.compare(snap_name) == 0) {
            if (current.snap_status != SnapStatus::SNAP_CREATING) {
                LOG_ERROR << "snap:" << snap_name << "no in creating";
                return StatusCode::sSnapAlreadyExist;
            }
            current.snap_status = SnapStatus::SNAP_CREATED; 
            m_index_store->db_put(current.key(), current.encode());
            break;
        }
    }
    return StatusCode::sOk;
}

StatusCode SnapshotMds::delete_event_update_status(const std::string& snap_name) {
    if (!check_snap_exist(snap_name)) {
        LOG_ERROR << "snap:" << snap_name << "no exist";
        return StatusCode::sSnapNotExist;
    }
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    snap_id_t snap_id = get_snap_id(snap_name);
    assert(snap_id != -1);
    snap_t current;
    current.snap_id = snap_id;
    transaction->del(current.key());
    /*locate current snapshot cow blocks*/
    cow_block_t cow_block;
    cow_block.snap_id = snap_id;
    it->seek_to_first(cow_block.prefix());
    for (; it->valid() && (-1 != it->key().find(cow_block.prefix())); it->next()) {
        /*maintain cow block ref */
        cow_block_t cur_block;
        cur_block.decode(it->value());
        cow_block_ref_t cur_block_ref;
        cur_block_ref.block_url = cur_block.block_url;
        std::string cur_block_ref_val = m_index_store->db_get(cur_block_ref.key());
        cur_block_ref.decode(cur_block_ref_val);
        cur_block_ref.block_url_refs.erase(snap_id);
        if (cur_block_ref.block_url_refs.empty()) {
            transaction->del(cur_block_ref.key()); 
            m_block_store->remove(cur_block_ref.block_url);
        } else {
            transaction->put(cur_block_ref.key(), cur_block_ref.encode()); 
        }
        transaction->del(it->key()); 
    }
    int ret = m_index_store->submit_transaction(transaction);
    if (ret) {
        LOG_ERROR << "snap:" << snap_name << " submit transaction failed";
        return StatusCode::sSnapMetaPersistError;
    }
    trace();
    LOG_INFO << "update sname:" << snap_name << " delete event ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::rollback_event_update_status(const std::string& snap_name) {
    if (!check_snap_exist(snap_name)) {
        LOG_ERROR << "snap:" << snap_name << "no exist";
        return StatusCode::sSnapNotExist;
    }
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid() && (-1 != it->key().find(snap_prefix)); it->next()) {
        snap_t current;
        current.decode(it->value());
        if (current.snap_name.compare(snap_name) == 0) {
            current.snap_status = SnapStatus::SNAP_ROLLBACKING; 
            m_index_store->db_put(current.key(), current.encode());
            break;
        }
    }
    return StatusCode::sOk;
}

std::string SnapshotMds::mapping_snap_name(const SnapReqHead& shead,
                                           const std::string& sname) {
    if (shead.replication_uuid().empty() && shead.checkpoint_uuid().empty() &&
        shead.snap_type() == SnapType::SNAP_LOCAL) {
        /*local snapshot */
        return sname;
    }
    /*remote snapshot, only check checkpoint_uuid same*/
    const std::string& checkpoint = shead.checkpoint_uuid();
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(snap_prefix);
    for (; it->valid() && (-1 != it->key().find(snap_prefix)); it->next()) {
        snap_t current;
        current.decode(it->value());
        if (current.ckp_id.compare(checkpoint) == 0) {
            return current.snap_name;
        }
    }
    return "";
}

StatusCode SnapshotMds::update(const UpdateReq* req, UpdateAck* ack) {
    std::string vol_name = req->vol_name();
    std::string snap_name = req->snap_name();
    UpdateEvent snap_event = req->snap_event();
    LOG_INFO << "update vname:" << vol_name << " sname:" << snap_name
             << " event:" << snap_event;
    StatusCode ret = StatusCode::sOk;
    std::string cur_snap_name = mapping_snap_name(req->header(), snap_name);
    do {
        if (cur_snap_name.empty()) {
            ret = StatusCode::sSnapNotExist;
            break;
        }
        if (!check_snap_exist(cur_snap_name)) {
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
            case UpdateEvent::ROLLBACKING_EVENT:
                rollback_event_update_status(cur_snap_name);
            case UpdateEvent::ROLLBACKED_EVENT:
                break;
            default:
                ret = StatusCode::sSnapUpdateError;
                break;
        }
    } while(0);
    ack->mutable_header()->set_status(ret);
    trace();
    /*get the latest snapshot in system*/
    std::string latest_snap_name = get_latest_snap_name();
    ack->set_latest_snap_name(latest_snap_name);
    LOG_INFO << "update vname:" << vol_name << " sname:" << snap_name
             << " event:" << snap_event << " latest_snap_name:" << latest_snap_name << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::cow_op(const CowReq* req, CowAck* ack) {
    std::string vname = req->vol_name();
    std::string snap_name = req->snap_name();
    block_t blk_id = req->blk_no();
    snap_id_t snap_id = get_snap_id(snap_name);
    assert(snap_id != -1);
    LOG_INFO << "cow op vname:" << vname << " snap_name:" << snap_name
             << " snap_id:" << snap_id << " blk_id:"  << blk_id;
    /*locate snapshot cow block*/ 
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    cow_block_t cur_pivot_block;
    cur_pivot_block.snap_id = snap_id;
    it->seek_to_first(cur_pivot_block.prefix());
    for (; it->valid() && (-1 != it->key().find(cur_pivot_block.prefix())); it->next()) {
        cow_block_t cur_cow_block;
        cur_cow_block.decode(it->value());
        if (cur_cow_block.block_id == blk_id) {
            /*block already cow*/
            LOG_INFO << "cow op block:" << blk_id << " block_url:"
                     << cur_cow_block.block_url << " already cow";
            ack->mutable_header()->set_status(StatusCode::sOk);
            ack->set_cow_op(COW_NO);
            return StatusCode::sOk;
        }
    }
    /*create cow object*/
    std::string blk_url = spawn_block_url(snap_id, blk_id);
    ack->mutable_header()->set_status(StatusCode::sOk);
    ack->set_cow_op(COW_YES);
    ack->set_blk_url(blk_url);
    LOG_INFO << "cow op block:" << blk_id << " block_url:" << blk_url << " need cow";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::cow_update(const CowUpdateReq* req, CowUpdateAck* ack) {
    std::string vname = req->vol_name();
    std::string snap_name = req->snap_name();
    block_t blk_no = req->blk_no();
    bool blk_zero = req->blk_zero();
    std::string blk_url = req->blk_url();
    snap_id_t snap_id = get_snap_id(snap_name);
    assert(snap_id != -1);
    LOG_INFO << "cow update vname:" << vname << " snap_name:" << snap_name
             << " snap_id:" << snap_id << " blk_id:"  << blk_no
             << " blk_url:" << blk_url;
    IndexStore::Transaction transaction = m_index_store->fetch_transaction();
    cow_block_t cur_cow_block;
    cur_cow_block.snap_id = snap_id;
    cur_cow_block.block_id = blk_no;
    cur_cow_block.block_zero = blk_zero;
    cur_cow_block.block_url = blk_url;
    transaction->put(cur_cow_block.key(), cur_cow_block.encode());
    /*maintain cow block ref*/
    cow_block_ref_t cur_cow_block_ref;
    cur_cow_block_ref.block_url = blk_url;
    cur_cow_block_ref.block_url_refs.clear();
    cur_cow_block_ref.block_url_refs.insert(snap_id);
    /*backward add block ref to previous snapshot*/
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    bool loop = true;
    snap_id_t prev_snap_id = get_prev_snap_id(snap_id);
    while (loop && (-1 != prev_snap_id)) {
        cow_block_t prev_first_block;
        prev_first_block.snap_id = prev_snap_id;
        it->seek_to_first(prev_first_block.prefix());
        for (; it->valid() && (-1 != it->key().find(prev_first_block.prefix())); it->next()) {
            cow_block_t trip_block;
            trip_block.decode(it->value());
            if (trip_block.block_id == blk_no) {
                loop = false;
                break;
            }
        }
        if (loop) {
            cur_cow_block.snap_id = prev_snap_id;
            transaction->put(cur_cow_block.key(), cur_cow_block.encode());
            cur_cow_block_ref.block_url_refs.insert(prev_snap_id);
        }
        prev_snap_id = get_prev_snap_id(prev_snap_id);
    }
    transaction->put(cur_cow_block_ref.key(), cur_cow_block_ref.encode());

    int ret = m_index_store->submit_transaction(transaction);
    if (ret) {
        LOG_ERROR << "cow update submit transaction failed";
        return StatusCode::sSnapMetaPersistError;
    }
    trace();
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "cow update vname:" << vname << " snap_name:" << snap_name
             << " snap_id:" << snap_id << " blk_id:" << blk_no << " blk_zero:" << blk_zero
             << " blk_url:" << blk_url << " ok";
    return StatusCode::sOk;
}

std::string SnapshotMds::spawn_block_url(const snap_id_t snap_id,
                                         const block_t  blk_id) {
    std::string block_url = m_volume_name;
    block_url.append("@");
    block_url.append(std::to_string(snap_id));
    block_url.append("@");
    block_url.append(std::to_string(blk_id));
    block_url.append(".obj");
    return block_url; 
}

StatusCode SnapshotMds::diff_snapshot(const DiffReq* req, DiffAck* ack) {
    std::string vname = req->vol_name();
    std::string first_snap = req->first_snap_name();
    std::string last_snap = req->last_snap_name();

    LOG_INFO << "diff snapshot vname:" << vname
             << " first_snap:" << first_snap << " last_snap:"  << last_snap;
    snap_id_t first_snapid = get_snap_id(first_snap);
    snap_id_t last_snapid  = get_snap_id(last_snap);
    assert(first_snapid != -1 && last_snapid != -1);
    snap_id_t cur_snap_id = first_snapid;
    while (cur_snap_id < last_snapid) {
        snap_id_t next_snap_id = get_next_snap_id(cur_snap_id);
        cow_block_t cur_pivot_block;
        cur_pivot_block.snap_id = cur_snap_id;
        IndexStore::IndexIterator cur_it = m_index_store->db_iterator();
        cur_it->seek_to_first(cur_pivot_block.prefix());
        DiffBlocks* diffblocks = ack->add_diff_blocks();
        diffblocks->set_vol_name(vname);
        diffblocks->set_snap_name(get_snap_name(cur_snap_id));
        for (; cur_it->valid() && (-1 != cur_it->key().find(cur_pivot_block.prefix())); cur_it->next()) {
            bool block_same_in_two_snap = false;
            cow_block_t cur_block;
            cur_block.decode(cur_it->value());
            LOG_INFO << "cur_block_url:" << cur_block.block_url;
            cow_block_t next_pivot_block;
            next_pivot_block.snap_id = next_snap_id;
            IndexStore::IndexIterator next_it = m_index_store->db_iterator();
            next_it->seek_to_first(next_pivot_block.prefix());
            for (; next_it->valid() && (-1 != next_it->key().find(next_pivot_block.prefix())); next_it->next()) {
                cow_block_t next_block;
                next_block.decode(next_it->value());
                LOG_INFO << "next_block_url:" << next_block.block_url;
                if (cur_block.block_id == next_block.block_id &&
                    cur_block.block_url.compare(next_block.block_url) == 0) {
                    block_same_in_two_snap = true;
                    break;
                }
            }
            LOG_INFO << "cur_block_url:" << cur_block.block_url << " same:" << block_same_in_two_snap;
            if (!block_same_in_two_snap) {
                DiffBlock* diff_block = diffblocks->add_block();
                diff_block->set_blk_no(cur_block.block_id);
                diff_block->set_blk_zero(cur_block.block_zero);
                diff_block->set_blk_url(cur_block.block_url);
            }
        }

        cur_snap_id = next_snap_id;
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "diff snapshot" << " vname:" << vname
             << " first_snap:" << first_snap << " last_snap:"  << last_snap
             << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotMds::read_snapshot(const ReadReq* req, ReadAck* ack) {
    std::string vol_name = req->vol_name();
    std::string snap_name = req->snap_name();
    off_t off = req->off();
    size_t len = req->len();
    LOG_INFO << "read snapshot" << " vname:" << vol_name << " sname:" << snap_name
             << " off:"  << off << " len:"  << len;
    snap_id_t snap_id = get_snap_id(snap_name);
    assert(snap_id != -1);
    cow_block_t cur_pivot_block;
    cur_pivot_block.snap_id = snap_id;
    IndexStore::IndexIterator it = m_index_store->db_iterator();
    it->seek_to_first(cur_pivot_block.prefix());
    for (; it->valid() && (-1 != it->key().find(cur_pivot_block.prefix())); it->next()) {
        cow_block_t cur_block;
        cur_block.decode(it->value());
        off_t blk_start = cur_block.block_id * COW_BLOCK_SIZE;
        off_t blk_end = (cur_block.block_id+1) * COW_BLOCK_SIZE;
        if (blk_start >= off + len || blk_end <= off) {
            continue;
        }
        ReadBlock* rblock = ack->add_read_blocks();
        rblock->set_blk_no(cur_block.block_id);
        rblock->set_blk_zero(cur_block.block_zero);
        rblock->set_blk_url(cur_block.block_url);
        LOG_INFO << "read blk_no:" << cur_block.block_id << " blk_zero:" << cur_block.block_zero
                 << " blk_url:" << cur_block.block_url;
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "read snapshot" << " vname:" << vol_name << " sname:" << snap_name
             << " off:"  << off << " len:"  << len << " ok";
    return StatusCode::sOk;
}

int SnapshotMds::recover() {
    m_latest_snapid = get_latest_snap_id();
    return 0;
}

void SnapshotMds::trace() {
    LOG_INFO << "\t current snapshot info";
    IndexStore::IndexIterator snap_it = m_index_store->db_iterator();
    snap_it->seek_to_first(snap_prefix);
    for (; snap_it->valid() && (-1 != snap_it->key().find(snap_prefix)); snap_it->next()) {
        LOG_INFO << "\t\t" << snap_it->key() << " " << snap_it->value();
    }
    LOG_INFO << "\t current snapshot block info";
    IndexStore::IndexIterator block_it = m_index_store->db_iterator();
    block_it->seek_to_first(block_prefix);
    for (; block_it->valid() && (-1 != block_it->key().find(block_prefix)); block_it->next()) {
        LOG_INFO << "\t\t" << block_it->key() << " " << block_it->value();
    }
    LOG_INFO << "\t current snapshot block ref info";
    IndexStore::IndexIterator block_ref_it = m_index_store->db_iterator();
    block_ref_it->seek_to_first(block_ref_prefix);
    for (; block_ref_it->valid() && (-1 != block_ref_it->key().find(block_ref_prefix)); block_ref_it->next()) {
        LOG_INFO << "\t\t" << block_ref_it->key() << " " << block_ref_it->value();
    }
}
