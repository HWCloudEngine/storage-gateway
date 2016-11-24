/*
 * journal_replayer.cpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <cstdio>
#include <errno.h>
#include <boost/bind.hpp>
#include "journal_replayer.hpp"
#include "../log/log.h"

namespace Journal
{

JournalReplayer::JournalReplayer(const std::string& rpc_addr)
{

    rpc_client_ptr_.reset(
            new ReplayerClient(
                    grpc::CreateChannel(rpc_addr,
                            grpc::InsecureChannelCredentials())));
}

bool JournalReplayer::init(const std::string& vol_id, 
                           const std::string& device,
                           std::shared_ptr<IDGenerator> id_maker_ptr,
                           std::shared_ptr<CacheProxy> cache_proxy_ptr,
                           std::shared_ptr<SnapshotProxy> snapshot_proxy_ptr){
    vol_id_ = vol_id;
    device_ = device;

    id_maker_ptr_       = id_maker_ptr;
    cache_proxy_ptr_    = cache_proxy_ptr;
    snapshot_proxy_ptr_ = snapshot_proxy_ptr;
    exist_snapshot_     = false;

    cache_recover_ptr_.reset(new CacheRecovery(device_, 
                                rpc_client_ptr_, 
                                id_maker_ptr_,
                                cache_proxy_ptr_));
    //start recover
    cache_recover_ptr_->start();

    update_ = false;
    vol_fd_ = open(device_.c_str(), O_WRONLY | O_DIRECT | O_SYNC);
    if (vol_fd_ < 0)
    {
        LOG_ERROR << "open volume failed";
        return false;
    }
    //start replay volume
    replay_thread_ptr_.reset(
            new boost::thread(
                    boost::bind(&JournalReplayer::replay_volume, this)));
    //start update marker
    update_thread_ptr_.reset(
            new boost::thread(
                    boost::bind(&JournalReplayer::update_marker, this)));

    return true;
}

bool JournalReplayer::deinit()
{
    replay_thread_ptr_->interrupt();
    update_thread_ptr_->interrupt();
    replay_thread_ptr_->join();
    update_thread_ptr_->join();
    cache_recover_ptr_->stop();
    close(vol_fd_);
    return true;
}

//update marker
void JournalReplayer::update_marker()
{
    //todo: read config ini
    int_least64_t update_interval = 60;
    while (true)
    {
        boost::this_thread::sleep_for(boost::chrono::seconds(update_interval));
        if (latest_entry_.get() && update_)
        {
            std::unique_lock < std::mutex > ul(entry_mutex_);
            std::string file_name = latest_entry_->get_journal_file();
            off_t off = latest_entry_->get_journal_off();
            journal_marker_.set_cur_journal(file_name.c_str());
            journal_marker_.set_pos(off);
            update_consumer_marker();
            LOG_INFO << "update marker succeed";
            update_ = false;
        }
    }
}

//replay volume
void JournalReplayer::replay_volume()
{
    //todo: read config ini
    while (true)
    {
        std::shared_ptr<CEntry> entry = cache_proxy_ptr_->pop();
        if (entry->get_cache_type() == 0)
        {
            //replay from memory
            LOG_INFO << "replay from memory";
            bool succeed = process_cache(entry->get_journal_entry());
            if (succeed)
            {
                std::unique_lock < std::mutex > ul(entry_mutex_);
                latest_entry_ = entry;
                cache_proxy_ptr_->reclaim(entry);
                update_ = true;
            }
        } 
        else
        {
            //replay from journal file
            LOG_INFO << "replay from journal file";
            const std::string file_name = entry->get_journal_file();
            const off_t off = entry->get_journal_off();
            bool succeed = process_file(file_name, off);
            if (succeed)
            {
                std::unique_lock < std::mutex > ul(entry_mutex_);
                latest_entry_ = entry;
                cache_proxy_ptr_->reclaim(entry);
                update_ = true;
            }
        }
    }
}

bool JournalReplayer::handle_ctrl_cmd(log_header_t* log_head)
{
    /*handle snapshot*/
    if(log_head->type == SNAPSHOT_CREATE)
    {
        LOG_INFO << "journal_replayer create snapshot"
                 << " id:" << (unsigned)log_head->count;
        /*update snapshot status*/
        snapid_t snap_id = log_head->count;
        string   snap_name = snapshot_proxy_ptr_->local_get_snapshot_name(snap_id);
        snapshot_proxy_ptr_->do_create(snap_name, snap_id);
        exist_snapshot_ = true;
        latest_snapid_  = snap_id;
        return true;
    } 
    else if(log_head->type == SNAPSHOT_DELETE)
    {
        LOG_INFO << "journal_replayer delete snapshot" 
                 << " id:" << (unsigned)log_head->count;
        snapid_t snap_id = log_head->count;
        snapshot_proxy_ptr_->do_delete(snap_id);
        if(latest_snapid_ == snap_id){
            exist_snapshot_ = false;
        }
        return true;
    }
    else if(log_head->type == SNAPSHOT_ROLLBACK)
    {
        LOG_INFO << "journal_replay rollback snapshot"
                 << " id:" << (unsigned)log_head->count;
        snapid_t snap_id = log_head->count;
        snapshot_proxy_ptr_->do_rollback(snap_id);
        return true;
    } 
    else 
    {
        ;
    }

    return false;
}

bool JournalReplayer::process_cache(std::shared_ptr<ReplayEntry> r_entry)
{
    /*get header*/
    log_header_t* log_head = r_entry->header();
    off_t header_length = r_entry->header_length();
    off_t length = r_entry->length();
    
    /*handle ctrl cmd*/
    if(handle_ctrl_cmd(log_head)){
        LOG_INFO << "replay succeed";
        return true;
    }

    //todo: use direct-IO to optimize journal replay
    off_t off_len_start = sizeof(log_header_t);
    off_t body_start = 0;
    while (off_len_start < header_length && body_start < length)
    {
        /*get off len*/
        off_len_t* off_len =
                reinterpret_cast<off_len_t *>((char*) r_entry->header()
                        + off_len_start);
        uint64_t off = off_len->offset;
        uint32_t length = off_len->length;
        const char* data = r_entry->body() + body_start;

        //todo: check crc
        void *align_buf = nullptr;
        int ret = posix_memalign((void**)&align_buf, 512, length);
        if (ret)
        {
            LOG_ERROR << "posix malloc failed ";
            return false;
        }
        memcpy(align_buf, data, length);

        if(!exist_snapshot_)
        {
            ret = pwrite(vol_fd_, align_buf, length, off);
        } 
        else 
        {
            ret = snapshot_proxy_ptr_->do_cow(off, length, 
                                              (char*)align_buf, false); 
        }

        if (align_buf)
        {
            free(align_buf);
        }

        off_len_start += sizeof(off_len_t);
        body_start += length;
    }

    LOG_INFO << "replay succeed";
    return true;
}

//todo: unify this function, get ReplayEntry from journal file
bool JournalReplayer::process_file(const std::string& file_name, off_t off)
{
    int src_fd = open(file_name.c_str(), O_RDONLY);
    if (src_fd < 0)
    {
        LOG_ERROR << "open journal file failed";
        return false;
    }

    /*read header*/
    lseek(src_fd, off, SEEK_SET);
    log_header_t log_head;
    memset(&log_head, 0, sizeof(log_head));
    size_t head_size = sizeof(log_head);
    int ret = read(src_fd, &log_head, head_size);
    if (ret != head_size)
    {
        LOG_ERROR << "read log head failed ret=" << ret;
        close(src_fd);
        return false;
    }
    
    if(handle_ctrl_cmd(&log_head)){
        LOG_INFO << "replay succeed";
        close(src_fd);
        return true;
    }

    /*read off len */
    size_t off_len_size = log_head.count * sizeof(off_len_t);
    off_len_t* off_len = (off_len_t*) malloc(off_len_size);
    ret = read(src_fd, off_len, off_len_size);
    if (ret != off_len_size)
    {
        LOG_ERROR << "read log off len failed ret=" << ret;
        close(src_fd);
        return false;
    }

    /*read data and replay data*/
    uint8_t count = log_head.count;
    for (int i = 0; i < log_head.count; i++)
    {
        uint64_t off = off_len[i].offset;
        uint32_t length = off_len[i].length;
        char* data = nullptr;
        int ret = posix_memalign((void**)&data, 512, length);
        assert(ret == 0 && data != nullptr);
        ret = read(src_fd, data, length);
        if (ret != length)
        {
            LOG_ERROR << "read data failed ret=" << ret;
            close(src_fd);
            return false;
        }

        //todo: check crc
        if(!exist_snapshot_)
        {
            ret = pwrite(vol_fd_, data, length, off);
        } 
        else 
        {
            ret = snapshot_proxy_ptr_->do_cow(off, length, 
                                              (char*)data, false); 
        }

        if (data)
        {
            free(data);
        }
    }
    close(src_fd);
    if (off_len)
    {
        free(off_len);
    }
    LOG_INFO << "replay succeed";
    return true;
}

bool JournalReplayer::update_consumer_marker()
{
    if (rpc_client_ptr_->UpdateConsumerMarker(journal_marker_, vol_id_))
    {
        return true;
    } else
    {
        return false;
    }
}

}
