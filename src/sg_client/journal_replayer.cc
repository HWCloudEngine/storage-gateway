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
#include "journal_replayer.h"
#include "volume.h"

#include "../log/log.h"
#include "../rpc/message.pb.h"

using google::protobuf::Message;
using huawei::proto::WriteMessage;
using huawei::proto::SnapshotMessage;
using huawei::proto::DiskPos;

using namespace std;

namespace Journal
{

JournalReplayer::JournalReplayer(VolumeStatus& vol_status)
                :vol_status_(vol_status)
{
}

bool JournalReplayer::init(const std::string& vol_id, 
                           const std::string& device,
                           const std::string& rpc_addr,
                           std::shared_ptr<IDGenerator> id_maker_ptr,
                           std::shared_ptr<CacheProxy> cache_proxy_ptr,
                           std::shared_ptr<SnapshotProxy> snapshot_proxy_ptr)
{
    vol_id_ = vol_id;
    device_ = device;

    id_maker_ptr_       = id_maker_ptr;
    cache_proxy_ptr_    = cache_proxy_ptr;
    snapshot_proxy_ptr_ = snapshot_proxy_ptr;

    rpc_client_ptr_.reset(new ReplayerClient(grpc::CreateChannel(rpc_addr,
                            grpc::InsecureChannelCredentials())));

    cache_recover_ptr_.reset(new CacheRecovery(vol_id_, rpc_client_ptr_, 
                                id_maker_ptr_, cache_proxy_ptr_));
    cache_recover_ptr_->start();
    //block until recover finish
    cache_recover_ptr_->stop(); 
    cache_recover_ptr_.reset();

    update_ = false;
    vol_fd_ = ::open(device_.c_str(), O_WRONLY | O_DIRECT | O_SYNC);
    if (vol_fd_ < 0){
        LOG_ERROR << "open volume failed";
        return false;
    }

    //start replay volume
    replay_thread_ptr_.reset(
            new boost::thread(
                    boost::bind(&JournalReplayer::replay_volume_loop, this)));
    //start update marker
    update_thread_ptr_.reset(
            new boost::thread(
                    boost::bind(&JournalReplayer::update_marker_loop, this)));

    return true;
}

bool JournalReplayer::deinit()
{
    /*todo: here force exit, not rational*/
    replay_thread_ptr_->interrupt();
    update_thread_ptr_->interrupt();
    replay_thread_ptr_->join();
    update_thread_ptr_->join();
    if(-1 != vol_fd_){
        ::close(vol_fd_);
    }
    return true;
}

void JournalReplayer::update_marker_loop()
{
    //todo: read config ini
    int_least64_t update_interval = 60;
    while (true){
        boost::this_thread::sleep_for(boost::chrono::seconds(update_interval));
        if (update_){
            rpc_client_ptr_->UpdateConsumerMarker(journal_marker_, vol_id_);
            LOG_INFO << "update marker succeed";
            update_ = false;
        }
    }
}

bool JournalReplayer::replay_each_journal(const string& journal, const off_t& pos)
{
    bool retval = true;
    int  fd = -1;

    do {
        fd = ::open(journal.c_str(), O_RDONLY);
        if(-1 == fd){
            LOG_ERROR << "open " << journal.c_str() << "failed errno:" << errno;
            retval = false;
            break;
        }
        struct stat buf = {0};
        int ret = fstat(fd, &buf);
        if(-1 == ret){
            LOG_ERROR << "stat " << journal.c_str() << "failed errno:" << errno;
            retval = false; 
            break;
        }

        size_t size = buf.st_size;
        if(size == 0){
            break; 
        }

        off_t start = pos;
        off_t end   = size;
        LOG_INFO << "open file:" << journal<< " start:" << start 
                 << " size:" << size;

        while(start < end){
            string journal_file = journal;
            off_t  journal_off  = start;
            shared_ptr<JournalEntry> journal_entry = make_shared<JournalEntry>();
            start = journal_entry->parse(fd, start); 

            process_journal_entry(journal_entry);

            update_consumer_marker(journal_file, journal_off);
        }

    } while(0);
    
    if(-1 != fd){
        ::close(fd); 
    }
    return retval;
}

void JournalReplayer::replica_replay()
{
    while(true){
        /*get replayer consumer mark*/
        JournalMarker replay_consumer_mark;
        bool ret = rpc_client_ptr_->GetJournalMarker(vol_id_, replay_consumer_mark); 
        if(!ret){
            LOG_ERROR << "get journal replay consumer marker failed";
            return;
        }
        
        /*replay journal marker point journal*/
        string journal = replay_consumer_mark.cur_journal();
        off_t  pos = replay_consumer_mark.pos();
        replay_each_journal(journal, pos);    

        /*get other journal file list */
        constexpr int limit = 10;
        list<string> journal_list; 
        ret = rpc_client_ptr_->GetJournalList(vol_id_, replay_consumer_mark, 
                                              limit, journal_list);
        if(!ret || journal_list.empty()){
            LOG_ERROR << "get journal list failed";
            /*1. fail over occur 
             *2. no journal file any more
             *exist replica replay
             */
            if(vol_status_.is_failover_){
                return; 
            }
            usleep(200);
            continue;
        }
        
        /*replay journal file*/
        for(auto it : journal_list){
            journal = "/mnt/cephfs" + it;
            pos     = sizeof(journal_file_header_t);
            replay_each_journal(journal, pos);    
        }

        if(limit == journal_list.size()){
            /*more journal file again, renew latest marker*/ 
            auto rit = journal_list.rbegin();
            replay_consumer_mark.set_cur_journal(*rit);
            replay_consumer_mark.set_pos(0);
        } else {
            usleep(200);
            continue;
        }
    }    
}

void JournalReplayer::normal_replay()
{
    while (true){
        shared_ptr<CEntry> entry = cache_proxy_ptr_->pop();
        if(entry == nullptr){
            usleep(200);
            continue;
        }

        if (entry->get_cache_type() == CEntry::IN_MEM){
            //replay from memory
            LOG_INFO << "replay from memory";
            bool succeed = process_memory(entry->get_journal_entry());
            if (succeed){
                update_consumer_marker(entry->get_journal_file(), 
                                       entry->get_journal_off());
                cache_proxy_ptr_->reclaim(entry);
            }
        } else {
            //replay from journal file
            LOG_INFO << "replay from journal file";
            bool succeed = process_file(entry);
            if (succeed){
                update_consumer_marker(entry->get_journal_file(), 
                                       entry->get_journal_off());
                cache_proxy_ptr_->reclaim(entry);
            }
        }
    }
}

void JournalReplayer::replay_volume_loop()
{
    while (true){
        if(!vol_status_.is_master_ && !vol_status_.is_failover_){
            /*slave and no failover occur*/ 
            replica_replay();
        } else {
            /*1. master 
             *2. slave and failover occur*/
            normal_replay();
        }
    }
}

bool JournalReplayer::handle_io_cmd(shared_ptr<JournalEntry> entry)
{
    shared_ptr<Message> message = entry->get_message();
    shared_ptr<WriteMessage> write = dynamic_pointer_cast<WriteMessage>(message);

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

bool JournalReplayer::handle_ctrl_cmd(shared_ptr<JournalEntry> entry)
{
    /*handle snapshot*/
    int type = entry->get_type();
    if(type == SNAPSHOT_CREATE){
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<SnapshotMessage> snap_message = dynamic_pointer_cast
                                                    <SnapshotMessage>(message);
        string snap_name = snap_message->snap_name();
        LOG_INFO << "journal_replayer create snapshot:" << snap_name;
        snapshot_proxy_ptr_->create_transaction(snap_name);
        return true;
    } else if(type == SNAPSHOT_DELETE){
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<SnapshotMessage> snap_message = dynamic_pointer_cast
                                                    <SnapshotMessage>(message);
        string snap_name = snap_message->snap_name();
        LOG_INFO << "journal_replayer delete snapshot:" << snap_name;
        snapshot_proxy_ptr_->delete_transaction(snap_name);
        return true;
    } else if(type == SNAPSHOT_ROLLBACK) {
        shared_ptr<Message> message = entry->get_message();
        shared_ptr<SnapshotMessage> snap_message = dynamic_pointer_cast
                                                    <SnapshotMessage>(message);
        string snap_name = snap_message->snap_name();
        LOG_INFO << "journal_replayer rollback snapshot:" << snap_name;
        snapshot_proxy_ptr_->rollback_transaction(snap_name);
        return true;
    } else {
        ;
    }

    return false;
}

bool JournalReplayer::process_journal_entry(shared_ptr<JournalEntry> entry)
{
    journal_event_type_t type = entry->get_type();
    if(IO_WRITE == type){
        handle_io_cmd(entry);
    } else {
        handle_ctrl_cmd(entry);
    }

    return true;
}

bool JournalReplayer::process_memory(std::shared_ptr<JournalEntry> entry)
{
   return process_journal_entry(entry);
}

bool JournalReplayer::process_file(shared_ptr<CEntry> entry)
{
    string file_name = entry->get_journal_file();
    off_t  off = entry->get_journal_off();

    /*todo avoid open frequently*/
    int src_fd = ::open(file_name.c_str(), O_RDONLY);
    if (src_fd == -1){
        LOG_ERROR << "open journal file failed";
        return false;
    }
    
    shared_ptr<JournalEntry> jentry = make_shared<JournalEntry>();
    jentry->parse(src_fd, off);
       
    if(src_fd != -1){
        ::close(src_fd); 
    }
    
    return process_journal_entry(jentry);
}

void JournalReplayer::update_consumer_marker(const string& journal,
                                             const off_t&  off)
{
    std::unique_lock<std::mutex> ul(journal_marker_mutex_);
    journal_marker_.set_cur_journal(journal.c_str());
    journal_marker_.set_pos(off);
    LOG_INFO << "consumer marker file:" << journal << " pos:" << off;
    update_ = true;
}

}
