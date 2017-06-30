/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    snapshot.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot interface
* *************************************************/
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <set>
#include <algorithm>
#include <chrono>
#include <fstream>
#include "common/interval_set.h"
#include "common/utils.h"
#include "common/config_option.h"
#include "rpc/message.pb.h"
#include "common/utils.h"
#include "snapshot_proxy.h"

using huawei::proto::SnapshotMessage;
using huawei::proto::SnapScene;
using huawei::proto::VolumeStatus;
using huawei::proto::RepStatus;
using huawei::proto::RepRole;

#define ALIGN_UP(v, align) (((v)+(align)-1) & ~((align)-1))

SnapshotProxy::SnapshotProxy(VolumeAttr& vol_attr, BlockingQueue<shared_ptr<JournalEntry>>& entry_queue)
    :m_vol_attr(vol_attr), m_entry_queue(entry_queue) {
    m_snap_rpc_cli = nullptr;
    init();
    LOG_INFO << "create proxy vname:" << m_vol_attr.vol_name() << " ok";
}

SnapshotProxy::~SnapshotProxy() {
    fini();
    LOG_INFO << "delete proxy vname:" << m_vol_attr.vol_name() << " ok";
}

bool SnapshotProxy::init() {
    /*open block device */
    Env::instance()->create_access_file(m_vol_attr.blk_device(), true,
                                        &m_block_file);
    if (m_block_file.get() == nullptr) {
        LOG_ERROR << "create block device:" << m_vol_attr.blk_device() << " failed";
        return false;
    }
    /*snapshot block store*/
    m_block_store = new CephBlockStore(g_option.ceph_cluster_name,
                                       g_option.ceph_user_name,
                                       g_option.ceph_pool_name);
    if (m_block_store == nullptr) {
        LOG_ERROR << "create block store failed";
        return false;
    }
    m_sync_table.clear();
    m_active_snapshot.clear();
    m_exist_snapshot = false;
    m_snap_rpc_cli = new SnapRpcCli();
    if (m_snap_rpc_cli == nullptr) {
        LOG_ERROR << "snap rpc client failed";
        return false;
    }
    /*sync with dr server*/
    m_snap_rpc_cli->do_init_sync(m_vol_attr.vol_name(), m_active_snapshot);
    if (!m_active_snapshot.empty()) {
        m_exist_snapshot = true;
    }
    return true;
}

bool SnapshotProxy::fini() {
    m_sync_table.clear();
    m_active_snapshot.clear();
    m_exist_snapshot = false;
    if (m_block_store) {
        delete m_block_store;
    }
    if (m_snap_rpc_cli) {
        delete m_snap_rpc_cli;
    }
    return true;
}

void SnapshotProxy::cmd_persist_wait() {
    unique_lock<std::mutex> ulock(m_cmd_persist_lock);
    m_cmd_persist_cond.wait(ulock, [&]() { 
        return true == m_cmd_persist_ok.load(std::memory_order_relaxed);
    });
    m_cmd_persist_ok.store(false, std::memory_order_relaxed);
}

void SnapshotProxy::cmd_persist_notify(const JournalMarker& mark) {
    unique_lock<std::mutex> ulock(m_cmd_persist_lock);
    m_cmd_persist_mark = mark;
    if (true == m_cmd_persist_ok.load(std::memory_order_relaxed)) {
        LOG_ERROR << "synchronize mechanism failed"; 
    }
    m_cmd_persist_ok.store(true, std::memory_order_relaxed);
    m_cmd_persist_cond.notify_all();
}

void SnapshotProxy::add_sync(const string& actor, const string& action) {
    m_sync_table.insert({actor, action});
}

void SnapshotProxy::del_sync(const string& actor) {
    m_sync_table.erase(actor);
}

bool SnapshotProxy::check_sync_on(const string& actor) {
    auto it = m_sync_table.find(actor);
    if (it == m_sync_table.end()) {
        return false;
    }
    return true;
}

StatusCode SnapshotProxy::create_snapshot(const CreateSnapshotReq* req,
                                          CreateSnapshotAck* ack) {
    JournalMarker m;
    return create_snapshot(req, ack, m);
}

StatusCode SnapshotProxy::create_snapshot(const CreateSnapshotReq* req,
                        CreateSnapshotAck* ack, JournalMarker& marker) {
    /*get from exterior rpc*/
    string vname = req->vol_name();
    string sname = req->snap_name();
    SnapType snap_type = req->header().snap_type();
    LOG_INFO << "create_snapshot vname:" << vname << " sname:" << sname;
    if (!m_vol_attr.is_snapshot_allowable(snap_type)) {
        LOG_ERROR << "create_snapshot vname:" << vname
                  << " sname:" << sname << " denied";
        return StatusCode::sSnapCreateDenied;
    }
    StatusCode ret_code = StatusCode::sOk;
    /*sync begin*/
    add_sync(sname, "snapshot on creating");
    if (m_vol_attr.is_append_entry_need(snap_type)) {
        /*spawn journal entry*/
        shared_ptr<JournalEntry> entry = spawn_journal_entry(req->header(),
                                                sname, SNAPSHOT_CREATE);
        /*push journal entry to entry queue*/
        m_entry_queue.push(entry);
        /*todo: wait journal writer persist journal entry ok and ack*/
        cmd_persist_wait();

        marker.CopyFrom(m_cmd_persist_mark);
        LOG_INFO << "create_snapshot vname:" << vname << " sname:" << sname
                 << " journal:" << m_cmd_persist_mark.cur_journal()
                 << " pos:" << m_cmd_persist_mark.pos();
    }
    /*rpc with dr_server */
    ret_code = m_snap_rpc_cli->do_create(req->header(), m_vol_attr.vol_name(), sname);
    LOG_INFO << "create_snapshot vname:" << vname << " sname:" << sname
             << (!ret_code ? " ok" : " failed, rpc error");
    /*sync end*/
    del_sync(sname);
    ack->mutable_header()->set_status(ret_code);
    return ret_code;
}

StatusCode SnapshotProxy::list_snapshot(const ListSnapshotReq* req,
                                        ListSnapshotAck* ack) {
    string vname = req->vol_name();
    LOG_INFO << "list_snapshot vname:" << vname;
    std::set<std::string> snaps; 
    auto ret = m_snap_rpc_cli->do_list(vname, snaps);
    for (auto snap : snaps) {
       ack->add_snap_name(snap);
    }
    LOG_INFO << "list_snapshot vname:" << vname
             << " snap_size:" << snaps.size() << " ret:" << ret;
    return ret;
}

StatusCode SnapshotProxy::query_snapshot(const QuerySnapshotReq* req,
                                         QuerySnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "query_snapshot vname:" << vname << " sname:" << sname;
    SnapStatus snap_status;    
    auto ret = m_snap_rpc_cli->do_query(vname, sname, snap_status);
    ack->set_snap_status(snap_status);
    LOG_INFO << "query_snapshot vname:" << vname << " sname:" << sname << " ret:" << ret;
    return ret;
}

StatusCode SnapshotProxy::delete_snapshot(const DeleteSnapshotReq* req,
                                          DeleteSnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    SnapType snap_type = req->header().snap_type();
    LOG_INFO << "delete_snapshot vname:" << vname << " sname:" << sname;
    /*sync begin*/
    add_sync(sname, "snapshot on deleting");
    if (m_vol_attr.is_append_entry_need(snap_type)) {
        /*spawn journal entry*/
        shared_ptr<JournalEntry> entry = spawn_journal_entry(req->header(),
                                                    sname, SNAPSHOT_DELETE);
        /*push journal entry to write queue*/
        m_entry_queue.push(entry);
        cmd_persist_wait();
    }
    /*rpc with dr_server*/
    auto ret = m_snap_rpc_cli->do_delete(req->header(), m_vol_attr.vol_name(), sname);
    LOG_INFO << "delete_snapshot vname:" << vname << " sname:" << sname
             << (!ret ? " ok" : " failed, rpc error");
    /*sync end*/
    del_sync(sname);
    ack->mutable_header()->set_status(ret);
    return ret;
}

StatusCode SnapshotProxy::rollback_snapshot(const RollbackSnapshotReq* req,
                                            RollbackSnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    SnapType snap_type = req->header().snap_type();
    LOG_INFO << "rollback_snapshot vname:" << vname << " sname:" << sname;
    add_sync(sname, "snapshot on rollbacking");
    if (m_vol_attr.is_append_entry_need(snap_type)) {
        /*spawn journal entry*/
        shared_ptr<JournalEntry> entry = spawn_journal_entry(req->header(),
                                                  sname, SNAPSHOT_ROLLBACK);
        /*push journal entry to write queue*/
        m_entry_queue.push(entry);
        /*wait journal writer persist journal entry ok and ack*/
        cmd_persist_wait();
    }
    auto ret = update(req->header(), sname, UpdateEvent::ROLLBACKING_EVENT);
    LOG_INFO << "rollback_snapshot vname:" << vname << " sname:" << sname
             << (!ret ? " ok" : " failed, rpc error");
    del_sync(sname);
    return ret;
}

StatusCode SnapshotProxy::create_transaction(const SnapReqHead& shead,
                                             const string& snap_name) {
    LOG_INFO << "create transaction sname:" << snap_name << " begin";
    auto ret = transaction(shead, snap_name, UpdateEvent::CREATE_EVENT);
    if (ret) {
        /*create snapshot failed, delete the snapshot*/
        ret = update(shead, snap_name, UpdateEvent::DELETE_EVENT);
        LOG_ERROR << "create transaction sname:" << snap_name << " failed";
        return ret;
    }
    LOG_INFO << "create transaction sname:" << snap_name << " end";
    return ret;
}

StatusCode SnapshotProxy::delete_transaction(const SnapReqHead& shead,
                                             const string& snap_name) {
    LOG_INFO << "delete transaction sname:" << snap_name << " begin";
    auto ret = transaction(shead, snap_name, UpdateEvent::DELETE_EVENT);
    if (ret) {
        LOG_ERROR << "delete transaction sname:" << snap_name << " failed";
        return ret;
    }
    LOG_INFO << "delete transaction sname:" << snap_name << " end";
    return ret;
}

StatusCode SnapshotProxy::rollback_transaction(const SnapReqHead& shead,
                                               const string& snap_name) {
    LOG_INFO << "rollback transaction sname:" << snap_name << " begin";
    auto ret = transaction(shead, snap_name, UpdateEvent::ROLLBACKED_EVENT);
    if (ret) {
        LOG_ERROR << "rollback transaction sname:" << snap_name << " failed";
        return ret;
    }
    std::vector<RollBlock> blocks;
    ret = m_snap_rpc_cli->do_rollback(shead, m_vol_attr.vol_name(), snap_name, blocks);
    for (auto block : blocks) {
        LOG_INFO << "do rollback blk_no:" << block.blk_no() << " blk_zero:" << block.blk_zero()
                 << " blk_url:" << block.blk_url();
        /*read latest data from block device*/
        off_t  block_off = block.blk_no() * COW_BLOCK_SIZE;
        size_t block_size = COW_BLOCK_SIZE;
        char* block_buf = (char*)malloc(block_size);
        ssize_t read_ret = m_block_file->read(block_buf, block_size, block_off);
        assert(read_ret == COW_BLOCK_SIZE);
        /*do cow*/
        ret = cow_op(block_off, block_size, block_buf, true);
        assert(ret == StatusCode::sOk);
        free(block_buf);
        /*rollback*/
        string roll_block_url = block.blk_url();
        bool roll_block_zero = block.blk_zero(); 
        char* roll_buf = (char*)malloc(block_size);
        if (!roll_block_zero) {
            read_ret = m_block_store->read(roll_block_url, roll_buf, block_size, 0);
            assert(read_ret == block_size);
        } else {
            memset(roll_buf, 0, block_size);
        }
        ssize_t write_ret = m_block_file->write(roll_buf, block_size, block_off);
        assert(write_ret == COW_BLOCK_SIZE);
        free(roll_buf);
    }
    /*dr server to delete rollback snapshot*/
    ret = update(shead, snap_name, UpdateEvent::DELETE_EVENT);
    LOG_INFO << "rollback transaction sname:" << snap_name << " end";
    return ret;
}

shared_ptr<JournalEntry> SnapshotProxy::spawn_journal_entry(const SnapReqHead& shead,
                   const string& sname, const journal_event_type_t& entry_type) {
    /*spawn snapshot message*/
    shared_ptr<SnapshotMessage> message = make_shared<SnapshotMessage>();
    message->set_replication_uuid(shead.replication_uuid());
    message->set_checkpoint_uuid(shead.checkpoint_uuid());
    message->set_vol_name(m_vol_attr.vol_name());
    message->set_snap_scene(shead.scene());
    message->set_snap_type(shead.snap_type());
    message->set_snap_name(sname);
    /*spawn journal entry*/
    shared_ptr<JournalEntry> entry = make_shared<JournalEntry>();
    entry->set_type(entry_type);
    entry->set_message(message);
    return entry;
}

StatusCode SnapshotProxy::transaction(const SnapReqHead& shead,
                                      const std::string& sname,
                                      const UpdateEvent& sevent) {
    auto ret = StatusCode::sOk;
    while (check_sync_on(sname)) {
        usleep(200);
    }
    /*trigger dr server update snapshot status*/
    ret = update(shead, sname, sevent);
    if (ret) {
        return StatusCode::sSnapTransactionError;
    }
    return ret;
}

bool SnapshotProxy::check_exist_snapshot()const {
    return (!m_active_snapshot.empty() && m_exist_snapshot) ? true : false;
}

StatusCode SnapshotProxy::update(const SnapReqHead& shead, const std::string& sname,
                                 const UpdateEvent& sevent) {
    LOG_INFO << "do_update snap_name:" << sname << " event:" << sevent;
    auto ret = m_snap_rpc_cli->do_update(shead, m_vol_attr.vol_name(), sname,
                                         sevent, m_active_snapshot);
    if (m_active_snapshot.empty()) {
        m_exist_snapshot = false;
    } else {
        m_exist_snapshot = true;
    }
    LOG_INFO << "do_update snap_name:" << sname << " event:" << sevent << " ok";
    return StatusCode::sOk;
}

void SnapshotProxy::split_cow_block(const off_t& off, const size_t& size,
                                    vector<cow_block_t>& cow_blocks) {
    off_t start = off;
    off_t end = off + size;
    size_t len = size;
    block_t cur_blk_no;
    size_t split_size;
    while (start < end && len > 0) {
       if (start % COW_BLOCK_SIZE == 0) {
            cur_blk_no = start / COW_BLOCK_SIZE;
            split_size = min(len, COW_BLOCK_SIZE);
        } else {
            cur_blk_no = start / COW_BLOCK_SIZE;
            split_size = min(len, ((cur_blk_no+1)*COW_BLOCK_SIZE) - start);
        }
        cow_block_t cow_block;
        cow_block.off = start;
        cow_block.len = split_size;
        cow_block.blk_no = cur_blk_no;
        cow_blocks.push_back(cow_block);
        start += split_size;
        len -= split_size;
    }
}

StatusCode SnapshotProxy::cow_op(const off_t& off, const size_t& size,
                                 char* buf, bool rollback) {
    vector<cow_block_t> cow_blocks;
    split_cow_block(off, size, cow_blocks);
    assert(!cow_blocks.empty());
    LOG_INFO << "cow op snap_id:" << m_active_snapshot << " off:" << off
             << " size:" << size << " rollback:" << rollback;
    for (auto cow_block : cow_blocks) {
        LOG_INFO << "cow op off:" << cow_block.off << " len:" << cow_block.len
                 << " blk_no:" << cow_block.blk_no;
        cow_op_t op_code;
        std::string block_url;
        auto ret = m_snap_rpc_cli->do_cow_check(m_vol_attr.vol_name(),m_active_snapshot,
                                                cow_block.blk_no,  op_code, block_url);
        assert(ret.ok());
        /*io direct overlap*/
        if (op_code == COW_NO) {
            LOG_INFO << "cow op overlap";
            if (!rollback) {
                /*comon io write, overlap*/
                char*   block_buf = buf + cow_block.off - off;
                off_t   block_off = cow_block.off;
                size_t  block_len = cow_block.len;
                ssize_t write_ret = m_block_file->write(block_buf, block_len, block_off);
                assert(write_ret == block_len);
            }
            continue;
        }
        /*io cow read from block device*/
        off_t block_off = cow_block.blk_no * COW_BLOCK_SIZE;
        size_t block_size = COW_BLOCK_SIZE;
        char* block_buf = (char*)malloc(block_size);
        ssize_t read_ret = m_block_file->read(block_buf, block_size, block_off);
        assert(read_ret == block_size);
        LOG_INFO << "do cow read from block device";
        /*io cow write to cow object*/
        bool block_zero = mem_is_zero(block_buf, block_size);
        if (!block_zero) {
            int ret = m_block_store->write(block_url, block_buf, block_size, 0);
            assert(ret == 0);
        }
        free(block_buf);
        LOG_INFO << "do cow write to ceph rados block_zero:" << block_zero;
        /*io cow write new data to block device*/
        char* cow_buf = buf + cow_block.off - off;
        off_t cow_off = cow_block.off;
        size_t cow_len = cow_block.len;
        ssize_t write_ret = m_block_file->write(cow_buf, cow_len, cow_off);
        assert(write_ret == cow_len);
        LOG_INFO << "do cow write to block device";
        /*io cow update cow meta to dr server*/
        ret = m_snap_rpc_cli->do_cow_update(m_vol_attr.vol_name(), m_active_snapshot,
                                            cow_block.blk_no, block_zero, block_url);
    }

    LOG_INFO << "cow op snap_name:" << m_active_snapshot << " off:" << off
             << " size:" << size << " rollback:" << rollback << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotProxy::diff_snapshot(const DiffSnapshotReq* req,
                                        DiffSnapshotAck* ack) {
    string vname = req->vol_name();
    string first_snap = req->first_snap_name();
    string last_snap = req->last_snap_name();
    LOG_INFO << "diff_snapshot vname:" << vname << " first_snap:" << first_snap
             << " last_snap:"  << last_snap;
    std::vector<DiffBlocks> blocks; 
    auto ret = m_snap_rpc_cli->do_diff(req->header(), vname, first_snap, last_snap, blocks);
    for (auto block : blocks) {
        DiffBlocks* diff_blocks = ack->add_diff_blocks();
        diff_blocks->CopyFrom(block);
    }
    LOG_INFO << "diff_snapshot vname:" << vname << " first_snap:" << first_snap
             << " last_snap:"  << last_snap << " ok";
    return StatusCode::sOk;
}

StatusCode SnapshotProxy::read_snapshot(const ReadSnapshotReq* req,
                                        ReadSnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    off_t  off   = req->off();
    size_t len   = req->len();
    LOG_INFO << "read_snapshot vname:" << vname << " sname:" << sname
             << " off:" << off << " len:" << len;
    std::vector<ReadBlock> blocks;
    auto ret = m_snap_rpc_cli->do_read(req->header(), vname, sname, off, len, blocks);
    assert(ret.ok());
 
    char* read_buf = (char*)malloc(len);
    assert(read_buf != nullptr);
    /*--------first read----------------------*/
    interval_set<uint64_t> read_region;
    read_region.clear();
    read_region.insert(off, len);
    /*region read from orginal block device*/
    interval_set<uint64_t> read_device_region;
    read_device_region.clear();
    read_device_region.insert(off, len);
    /*region read from cow object*/
    interval_set<uint64_t> read_cowobj_region;
    read_cowobj_region.clear();
    /*cow block set in first read*/
    set<uint64_t> cow_block_set0;
    for (auto block : blocks) {
        uint64_t block_no = block.blk_no();
        bool block_zero = block.blk_zero();
        string block_url = block.blk_url();
        /*accumulate record which cow block */
        cow_block_set0.insert(block_no);
        /*accumulate record which read from cow object*/
        read_cowobj_region.insert(block_no * COW_BLOCK_SIZE, COW_BLOCK_SIZE);
        interval_set<uint64_t> block_region;
        block_region.insert(block_no * COW_BLOCK_SIZE, COW_BLOCK_SIZE);
        block_region.intersection_of(read_region);
        /*block region read from cow object*/
        if (!block_region.empty()) {
            for (interval_set<uint64_t>::iterator it = block_region.begin();
                 it != block_region.end(); it++) {
                char*  rbuf = read_buf + it.get_start() - off;
                size_t rlen = it.get_len();
                off_t  roff = it.get_start() - (block_no * COW_BLOCK_SIZE);
                size_t read_ret = 0;
                if (!block_zero) {
                    read_ret = m_block_store->read(block_url, rbuf, rlen, roff);
                    assert(read_ret == rlen);
                } else {
                    memset(rbuf, 0, rlen); 
                }
                LOG_INFO << "read_snapshot first read cow object"
                << " blk_no:" << block_no << " blk_ob:" << block_url
                << " start:" << it.get_start() << " len:"  << it.get_len()
                << " roff:" << roff << " rlen:" << rlen << " read_ret:" << read_ret;
           }
        }
    }
    /*compute which read from block device*/
    if (!read_cowobj_region.empty()) {
        read_device_region.subtract(read_cowobj_region);
    }
    if (!read_device_region.empty()) {
        for (interval_set<uint64_t>::iterator it = read_device_region.begin();
             it != read_device_region.end(); it++) {
            off_t  r_off = it.get_start();
            size_t r_len = it.get_len();
            char*  r_buf = read_buf + r_off - off;
            LOG_INFO << "read_snapshot first read block device"
                     << " cur_off:" << r_off
                     << " cur_len:" << r_len
                     << " align_off:" << ALIGN_UP(r_off, 512)
                     << " align_len:" << ALIGN_UP(r_len, 512);
            ssize_t read_ret = m_block_file->read(r_buf,
                                                  ALIGN_UP(r_len, 512),
                                                  ALIGN_UP(r_off, 512));
            assert(read_ret == r_len);
        }
    }

    /*----------second read---------------*/
    blocks.clear();
    ret = m_snap_rpc_cli->do_read(req->header(), vname, sname, off, len, blocks);
    assert(ret.ok());
    for (auto block : blocks) {
        uint64_t block_no = block.blk_no();
        bool block_zero = block.blk_zero();
        string block_url = block.blk_url();
        /*cow block has read during first second*/
        if (cow_block_set0.find(block_no) != cow_block_set0.end()) {
            continue;
        }
        /*when second read, some region in first read from block deivce should
         *read from new snapshot cow object*/
        interval_set<uint64_t> block_region;
        block_region.insert(block_no * COW_BLOCK_SIZE, COW_BLOCK_SIZE);
        block_region.intersection_of(read_device_region);
        /*block region read from cow object*/
        if (!block_region.empty()) {
            for (interval_set<uint64_t>::iterator it = block_region.begin();
                 it != block_region.end(); it++) {
                char*  rbuf = read_buf + it.get_start() - off;
                size_t rlen = it.get_len();
                off_t  roff = it.get_start() - (block_no * COW_BLOCK_SIZE);
                LOG_INFO << "read_snapshot second read cow object"
                    << " blk_no:" << block_no
                    << " blk_ob:" << block_url
                    << " cow_off:"  << it.get_start()
                    << " cow_len:"  << it.get_len();
                if (!block_zero) {
                    m_block_store->read(block_url, rbuf, rlen, roff);
                } else {
                    memset(rbuf, 0, rlen);
                }
            }
        }
    }
    ack->mutable_header()->set_status(StatusCode::sOk);
    ack->set_data(read_buf, len);
    if (read_buf) {
        free(read_buf);
    }
    LOG_INFO << "read_snapshot vname:" << vname << " sname:" << sname
             << " off:" << off << " len:" << len
             << " data size:" << ack->data().size()
             << " data_len:"  << ack->data().length() << " ok";
    return StatusCode::sOk;
}
