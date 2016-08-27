/*
 * journal_replayer.hpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_JOURNAL_REPLAYER_HPP_
#define JOURNAL_JOURNAL_REPLAYER_HPP_

#include <string>
#include <atomic>
#include <thread>
#include "../rpc/clients/replayer_client.hpp"
#include "./cache_recover/cache_recover.hpp"

namespace Journal {

class JournalReplayer {
    //todo
public:
    JournalReplayer(const std::string& rpc_addr, const std::string& vol_id,
            const std::string& device,
            std::shared_ptr<CacheProxy> cache_proxy_ptr);
    void start_recover();
    void stop_recover();

    void start_replay(int interval_time);
    void stop_replay();
private:
    void replay_thread(int interval_time);
    void replay_volume();
    bool replay_cache();
    bool replay_journal_file();
    bool process_file(const std::string& journal);

    bool get_journal_marker();
    bool get_journal_list(int limit);
    bool update_consumer_marker();

    std::string vol_id;
    std::string device;
    int replay_count;
    std::atomic_bool stop_atomic;

    JournalMarker journal_marker;
    std::list<std::string> journals;

    std::unique_ptr<std::thread> replay_thread_ptr;
    std::shared_ptr<ReplayerClient> rpc_client_ptr;
    std::unique_ptr<CacheRecovery> cache_recover_ptr;
    std::shared_ptr<CacheProxy> cache_proxy_ptr;
    std::shared_ptr<IDGenerator> id_maker_ptr;
};

}

#endif /* JOURNAL_JOURNAL_REPLAYER_HPP_ */
