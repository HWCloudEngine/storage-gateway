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
#include "../rpc/message.pb.h"

using google::protobuf::Message;
using huawei::proto::WriteMessage;
using huawei::proto::SnapshotMessage;
using huawei::proto::DiskPos;

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

    cache_recover_ptr_.reset(new CacheRecovery(vol_id_, rpc_client_ptr_, 
                                id_maker_ptr_, cache_proxy_ptr_));
    cache_recover_ptr_->start();
    //block until recover finish
    cache_recover_ptr_->stop(); 
    cache_recover_ptr_.reset();

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
        if(entry == nullptr){
            usleep(200);
            continue;
        }

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

bool JournalReplayer::write_block_device(shared_ptr<WriteMessage> write)
{
    int pos_num = write->pos_size();
    char* data = (char*)write->data().c_str();

    /*entry contain merged io*/
    for(int i = 0; i < pos_num; i++){
        DiskPos* pos = write->mutable_pos(i);
        off_t  off = pos->offset();
        size_t len = pos->length();

        /*todo: direct io need memory align*/
        void *align_buf = nullptr;
        int ret = posix_memalign((void**) &align_buf, 512, len);
        assert(ret == 0 && align_buf != nullptr);
        memcpy(align_buf, data, len);
        if(!snapshot_proxy_ptr_->check_exist_snapshot()){
            ret = pwrite(vol_fd_, align_buf, len, off);
        } else {
            ret = snapshot_proxy_ptr_->do_cow(off, len, (char*)align_buf, false);
        }
        free(align_buf);

        /*next io data offset*/
        data += len;
    }

    return true;
}

bool JournalReplayer::handle_ctrl_cmd(JournalEntry* entry)
{
    /*handle snapshot*/
    int type = entry->get_type();
    if(type == SNAPSHOT_CREATE){
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<SnapshotMessage> snap_message = dynamic_pointer_cast
                                                    <SnapshotMessage>(message);
        string   snap_name = snap_message->snap_name();
        LOG_INFO << "journal_replayer create snapshot:" << snap_name;
        snapshot_proxy_ptr_->create_transaction(snap_name);
        return true;
    } else if(type == SNAPSHOT_DELETE){
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<SnapshotMessage> snap_message = dynamic_pointer_cast
                                                    <SnapshotMessage>(message);
        string   snap_name = snap_message->snap_name();
        LOG_INFO << "journal_replayer delete snapshot:" << snap_name;
        snapshot_proxy_ptr_->delete_transaction(snap_name);
        return true;
    } else if(type == SNAPSHOT_ROLLBACK) {
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<SnapshotMessage> snap_message = dynamic_pointer_cast
                                                    <SnapshotMessage>(message);
        string   snap_name = snap_message->snap_name();
        LOG_INFO << "journal_replayer rollback snapshot:" << snap_name;
        snapshot_proxy_ptr_->rollback_transaction(snap_name);
        return true;
    } else {
        ;
    }

    return false;
}

bool JournalReplayer::process_cache(std::shared_ptr<JournalEntry> entry)
{
    journal_event_type_t type = entry->get_type();
    if(IO_WRITE == type){
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<WriteMessage> write_message = dynamic_pointer_cast
                                                    <WriteMessage>(message);
        write_block_device(write_message);
    } else {
        /*other message type*/ 
        handle_ctrl_cmd(entry.get());
    }

    return true;
}

//todo: unify this function, get ReplayEntry from journal file
bool JournalReplayer::process_file(const std::string& file_name, off_t off)
{
    /*todo avoid open frequently*/
    int src_fd = ::open(file_name.c_str(), O_RDONLY);
    if (src_fd == -1){
        LOG_ERROR << "open journal file failed";
        return false;
    }
    
    /*get journal entry from file*/
    JournalEntry entry;
    entry.parse(src_fd, off);
    journal_event_type_t type = entry.get_type();

    if(IO_WRITE == type){
        shared_ptr<Message> message = entry.get_message();
        shared_ptr<WriteMessage> write_message = dynamic_pointer_cast
                                                    <WriteMessage>(message);
        write_block_device(write_message);
    } else {
        /*other message type*/ 
        handle_ctrl_cmd(&entry);
    }
    
    if(src_fd != -1){
        ::close(src_fd); 
    }

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
