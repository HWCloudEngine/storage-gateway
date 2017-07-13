/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_writer.cc 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  handle writer io 
*
*************************************************/
#include <vector>
#include <list>
#include <cerrno>
#include "common/utils.h"
#include "rpc/message.pb.h"
#include "log/log.h"
#include "perf_counter.h"
#include "journal_writer.h"
using huawei::proto::SnapshotMessage;


JournalWriter::JournalWriter(BlockingQueue<shared_ptr<JournalEntry>>& write_queue,
                             BlockingQueue<io_reply_t*>& reply_queue,
                             VolumeAttr& vol_attr)
    :thread_ptr(), write_queue_(write_queue), reply_queue_(reply_queue),
     vol_attr_(vol_attr), cur_journal_size(0) ,marker_handler(vol_attr){
    LOG_INFO << "iowriter work thread create";
    cur_file_.reset();
    running_flag = false;
}

JournalWriter::~JournalWriter() {
    LOG_INFO << "iowriter work thread destory";
    cur_file_.reset();
    while (!seal_queue.empty()) {
        seal_journals(lease_client_->get_lease());
    }
}

bool JournalWriter::init(shared_ptr<IDGenerator> idproxy,
                         shared_ptr<CacheProxy> cacheproxy,
                         shared_ptr<SnapshotProxy> snapshotproxy,
                         shared_ptr<CephS3LeaseClient> lease_client,
                         int _epoll_fd) {
    idproxy_ = idproxy;
    cacheproxy_ = cacheproxy;
    snapshot_proxy_ = snapshotproxy;
    running_flag = true;
    vol_check_flag = true;
    lease_client_ = lease_client;
    cur_journal_size = 0;
    JournalElement e;
    e.set_journal("");
    cur_lease_journal = std::make_pair("", e);
    std::string meta_rpc_addr = rpc_address(g_option.meta_server_ip, g_option.meta_server_port);
    marker_handler.init(_epoll_fd, lease_client_);
    thread_ptr.reset(new boost::thread(boost::bind(&JournalWriter::work, this)));
    return true;
}

bool JournalWriter::deinit() {
    running_flag = false;
    write_queue_.stop();
    thread_ptr->join();
    return true;
}

void JournalWriter::work() {
    bool success = false;
    time_t start, end;
    shared_ptr<JournalEntry> entry = nullptr;
    uint64_t entry_size = 0;
    uint64_t write_size = 0;

    while (running_flag) {
        if(vol_check_flag && !write_journal_preprocess()){
            continue;
        }
        if (!write_queue_.pop(entry)) {
            continue;
        }

        do_perf(WRITE_BEGIN, entry->get_sequence());
        success = false;
        time(&start);
        time(&end);
        entry_size = entry->get_persit_size();

        while (!success && (difftime(end, start) < g_option.journal_write_timeout)) {
            // get journal file fd
            if (cur_file_.get() == nullptr
                || (entry_size + cur_journal_size) > g_option.journal_max_size) {
                to_seal_current_journal();
                invalid_current_journal();
                int res = get_next_journal();
                if (res == 0) {
                    res = open_current_journal();
                }
                else {
                    time(&end);
                    continue;
                }
                SG_ASSERT(true == write_journal_header());
                marker_handler.update_cached_marker(cur_lease_journal.second.journal(), cur_journal_size);
            }

            // validate journal lease
            if (!lease_client_->check_lease_validity(cur_lease_journal.first)) {
                invalid_current_journal();
                LOG_ERROR << "check lease validity result:false";
                time(&end);
                continue;
            }

            /*persist to journal file*/
            std::string journal_file = g_option.journal_mount_point + cur_lease_journal.second.path();
            off_t journal_off = cur_journal_size;
            write_size = entry->persist(&cur_file_, journal_off);
            if (write_size != entry_size) {
                LOG_ERROR << "write journal file: " << cur_lease_journal.second.path()
                          << " failed:" << strerror(errno);
                time(&end);
                continue;
            }
            /*clear message serialized data*/
            entry->clear_serialized_data();

            /*add to cache*/
            cacheproxy_->write(journal_file, journal_off, entry);

            // update journal offset
            cur_journal_size += write_size;
            success = true;

            synchronize_snapshot_cmd(entry);
            marker_handler.update_cached_marker(cur_lease_journal.second.journal(), cur_journal_size);
            marker_handler.update_written_size(write_size);
            seal_snapshot_journal(entry);
        }

        marker_handler.producer_update_trigger();
        do_perf(WRITE_END, entry->get_sequence());
        write_journal_response(entry, success);
    }
}

int JournalWriter::get_next_journal() {
    {
        std::unique_lock<std::recursive_mutex> journal_uk(journal_mtx_);
        if (journal_queue.empty()) {
            LOG_INFO << "journal_queue empty";
            get_writeable_journals(lease_client_->get_lease(), g_option.journal_limit);
        }

        if (journal_queue.empty()) {
            return -1;
        }

        cur_lease_journal = journal_queue.front();
        journal_queue.pop();
        LOG_INFO << "journal_queue pop journal:"
                 << cur_lease_journal.second.journal()
                 << ",path:" << cur_lease_journal.second.path();
    }
    cur_journal_size = 0;
    return 0;
}

int JournalWriter::open_current_journal() {
    if (cur_lease_journal.second.journal() == "") {
        return -1;
    }
    std::string tmp = g_option.journal_mount_point + cur_lease_journal.second.path();
    Env::instance()->create_access_file(tmp, false, &cur_file_);
    idproxy_->add_file(tmp);
    return 0;
}

int JournalWriter::to_seal_current_journal() {
    if (cur_lease_journal.first != "") {
        seal_queue.push(cur_lease_journal);
    }
    return 0;
}

int JournalWriter::close_current_journal_file() {
    cur_file_.reset();
    return 0;
}

bool JournalWriter::write_journal_header() {
    journal_file_header_t journal_header;
    ssize_t ret = cur_file_->write(reinterpret_cast<char*>(&journal_header),
                                   sizeof(journal_file_header_t));
    assert(ret == sizeof(journal_file_header_t));
    cur_journal_size += sizeof(journal_file_header_t);
    return true;
}

bool JournalWriter::get_writeable_journals(const std::string& uuid,
                                           const int32_t limit) {
    std::list<JournalElement> journals;
    int32_t tmp = 0;
    std::unique_lock<std::mutex> lk(rpc_mtx_);
    std::unique_lock<std::recursive_mutex> journal_uk(journal_mtx_);

    if (journal_queue.size() >= limit) {
       return true;
    } else {
        tmp = limit - journal_queue.size();
    }
    StatusCode ret = g_rpc_client.GetWriteableJournals(uuid, vol_attr_.vol_name(),
                                          tmp, journals);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "get journal file failed";
        return false;
    }

    for (auto tmp : journals) {
        journal_queue.push(std::make_pair(uuid, tmp));
    }
    return true;
}

bool JournalWriter::seal_journals(const std::string& uuid) {
    std::pair<std::string, JournalElement> tmp;
    std::list<std::string> journals;
    std::list<std::pair<std::string, JournalElement>> backup;

    std::unique_lock<std::mutex> lk(rpc_mtx_);
    std::unique_lock<std::mutex> seal_uk(seal_mtx_);

    while (!seal_queue.empty()) {
        tmp = seal_queue.front();
        // only seal these journals with valid lease
        if (tmp.second.journal() != "" && uuid == tmp.first) {
            journals.push_back(tmp.second.journal());
            backup.push_back(tmp);
        }
        seal_queue.pop();
    }

    if (!journals.empty()) {
        StatusCode ret = g_rpc_client.SealJournals(uuid, vol_attr_.vol_name(), journals);
        if (ret != StatusCode::sOk) {
            LOG_ERROR << "SealJournals failed";
            for (auto k : backup) {
                seal_queue.push(k);
            }
            return false;
        } else {
            LOG_INFO << "SealJournals ok";
            return true;
        }
    }
    return true;
}

void JournalWriter::send_reply(JournalEntry* entry, bool success) {
    vector<uint64_t> handles = entry->get_handle();
    for (uint64_t handle : handles) {
        io_reply_t* reply_ptr = reinterpret_cast<io_reply_t*>(new char[sizeof(io_reply_t)]);
        reply_ptr->magic = MESSAGE_MAGIC;
        reply_ptr->error = success?0:1;
        reply_ptr->seq   = entry->get_sequence();
        reply_ptr->handle = handle;
        reply_ptr->len = 0;
        if (!reply_queue_.push(reply_ptr)) {
            LOG_ERROR << "reply queue push failed";
            delete []reply_ptr;
            return;
        }
    }
}

void JournalWriter::invalid_current_journal() {
    JournalElement e;
    e.set_journal("");
    cur_lease_journal = std::make_pair("", e);
    cur_journal_size = 0;
    close_current_journal_file();
}

VolumeAttr& JournalWriter::get_vol_attr() {
    return vol_attr_;
}

MarkerHandler& JournalWriter::get_maker_handler(){
    return marker_handler;
}

bool JournalWriter::write_journal_preprocess(){
    /* wait until volume is writable
       update producer marker first when init, then the replicator
       could replicate the data written during last crashed/restart time */
    if(!vol_attr_.is_writable()){
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return false;
    }
    else{
        int res = get_next_journal();
        if (res != 0){
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            return false;
        } 
        else{
            res = open_current_journal();
            SG_ASSERT(0 == res);
            SG_ASSERT(true == write_journal_header());
            marker_handler.update_cached_marker(cur_lease_journal.second.journal(), cur_journal_size);
            vol_check_flag = false;
            return true;
        }
    }
}

void JournalWriter::write_journal_response(shared_ptr<JournalEntry> entry, bool success){
    if (entry->get_type() == IO_WRITE) {
        send_reply(entry.get(), success);
    } else if (entry->get_type() == SNAPSHOT_CREATE) {
        std::shared_ptr<SnapshotMessage> msg
            = std::dynamic_pointer_cast<SnapshotMessage>(entry->get_message());
        if (msg->snap_scene() == huawei::proto::FOR_REPLICATION_FAILOVER) {
            vol_check_flag = true;
            LOG_INFO << "snapshot for failover was insert, check volume["
                << vol_attr_.vol_name() << "] writable?";
            // TODO:connection should reject write io or BlockingQueue provides clear api
        }
    } else {
        ;
    }
}

void JournalWriter::synchronize_snapshot_cmd(shared_ptr<JournalEntry> entry){
    /*todo: unify callback framework
    snapshot cmd synchronize as soon as possible*/
    if (entry->get_type() == SNAPSHOT_CREATE ||
       entry->get_type() == SNAPSHOT_DELETE ||
       entry->get_type() == SNAPSHOT_ROLLBACK) {
        LOG_INFO << "journal write reply snapshot command";
        JournalMarker cur_write_mark;
        cur_write_mark.set_cur_journal(cur_lease_journal.second.journal());
        cur_write_mark.set_pos(cur_journal_size);
        snapshot_proxy_->cmd_persist_notify(cur_write_mark);
    }
}

void JournalWriter::seal_snapshot_journal(shared_ptr<JournalEntry> entry){
    // seal the journal while created a snapshot for replication
    if (entry->get_type() == SNAPSHOT_CREATE) {
        std::shared_ptr<SnapshotMessage> msg
            = std::dynamic_pointer_cast<SnapshotMessage>(entry->get_message());
        if (msg->snap_scene() >= huawei::proto::FOR_REPLICATION) {
            to_seal_current_journal();
            invalid_current_journal();
            LOG_INFO << "create snapshot for replication,seal current journal "
                     << cur_lease_journal.second.journal();
        }
    }
}

