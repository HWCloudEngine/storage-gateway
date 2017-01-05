/*
 * journal_replayer.hpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_JOURNAL_REPLAYER_HPP_
#define JOURNAL_JOURNAL_REPLAYER_HPP_

#include <string>
#include <boost/thread/thread.hpp>
#include <boost/noncopyable.hpp>
#include "seq_generator.hpp"
#include "cache/cache_proxy.h"
#include "cache/cache_recover.h"
#include "journal_entry.hpp"
#include "../rpc/clients/replayer_client.hpp"
#include "../rpc/message.pb.h"
#include "../snapshot/snapshot_proxy.h"
using google::protobuf::Message;
using huawei::proto::WriteMessage;

using namespace std;

namespace Journal
{

class VolumeStatus;

class JournalReplayer: private boost::noncopyable
{
public:
    explicit JournalReplayer(VolumeStatus& vol_status); 

    bool init(const string& vol_id, 
              const string& device,
              const string& rpc_addr,
              shared_ptr<IDGenerator> id_maker_ptr,
              shared_ptr<CacheProxy> cache_proxy_ptr,
              shared_ptr<SnapshotProxy> snapshot_proxy_ptr);
    bool deinit();

private:
    /*replay thread work function*/
    void replay_volume_loop();
    /*update marker thread work function*/
    void update_marker_loop();
    
    /*replay when slave is only replicate*/
    void replica_replay();
    /*replay when failover on slave*/
    void normal_replay();

    bool replay_each_journal(const string& journal, const off_t& pos);

    bool handle_io_cmd(shared_ptr<JournalEntry> entry);
    bool handle_ctrl_cmd(shared_ptr<JournalEntry> entry);

    bool process_journal_entry(shared_ptr<JournalEntry> entry);

    bool process_cache(shared_ptr<JournalEntry> entry);
    bool process_file(shared_ptr<CEntry> entry);

    void update_consumer_marker(const string& journal, const off_t& off);
    
    /*volume id and block device path*/
    string vol_id_;
    string device_;

    /*block device write fd*/ 
    int  vol_fd_;
    
    /*volume status shared with Volume class*/
    VolumeStatus& vol_status_;

    /*consumer marker*/
    std::mutex    journal_marker_mutex_;
    JournalMarker journal_marker_;
    bool update_;
    
    std::shared_ptr<ReplayerClient> rpc_client_ptr_;

    /*replay thread*/
    std::unique_ptr<boost::thread> replay_thread_ptr_;
    /*update mark thread*/
    std::unique_ptr<boost::thread> update_thread_ptr_;

    /*cache for replay*/
    std::shared_ptr<CacheProxy> cache_proxy_ptr_;

    /*cache recover when crash*/
    std::shared_ptr<IDGenerator> id_maker_ptr_;
    std::shared_ptr<CacheRecovery> cache_recover_ptr_;
    
    /*snapshot*/
    std::shared_ptr<SnapshotProxy> snapshot_proxy_ptr_;
};

}

#endif /* JOURNAL_JOURNAL_REPLAYER_HPP_ */
