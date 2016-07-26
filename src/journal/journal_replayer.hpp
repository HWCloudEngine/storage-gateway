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

namespace Journal {

class JournalReplayer {
    //todo
public:
    JournalReplayer(const std::string& rpc_addr, const std::string& vol_id);
    void start_replay(int interval_time);
    void stop_replay();
    void init_cache();
private:
    void replay(int interval_time);
    void replay_volume();
    void init_cache_thread();

    bool get_journal_marker();
    bool get_journal_list(int limit);
    bool update_consumer_marker();

    std::string vol_id;
    std::atomic_bool stop_atomic;
    std::shared_ptr<std::thread> replay_thread_ptr;

    ReplayerClient rpc_client;
    JournalMarker journal_marker;
    std::list<std::string> journals;
};

}

#endif /* JOURNAL_JOURNAL_REPLAYER_HPP_ */
