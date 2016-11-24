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

namespace Journal
{

class JournalReplayer: private boost::noncopyable
{
public:
    explicit JournalReplayer(const std::string& rpc_addr); 

    bool init(const std::string& vol_id, 
              const std::string& device,
              std::shared_ptr<IDGenerator> id_maker_ptr,
              std::shared_ptr<CacheProxy> cache_proxy_ptr,
              std::shared_ptr<SnapshotProxy> snapshot_proxy_ptr);
    bool deinit();
private:
    void replay_volume();
    void update_marker();
    bool write_block_device(shared_ptr<WriteMessage> write);
    bool process_cache(std::shared_ptr<JournalEntry> r_entry);
    bool process_file(const std::string& file_name, off_t off);
    bool update_consumer_marker();
    /*handle snapshot and other control command*/
    bool handle_ctrl_cmd(JournalEntry* entry);

    int vol_fd_;
    bool update_;
    std::mutex entry_mutex_;
    std::string vol_id_;
    std::string device_;
    JournalMarker journal_marker_;
    std::shared_ptr<CEntry> latest_entry_;
    std::shared_ptr<ReplayerClient> rpc_client_ptr_;

    /*actually replay thread*/
    std::unique_ptr<boost::thread> replay_thread_ptr_;
    /*mark update thread*/
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
