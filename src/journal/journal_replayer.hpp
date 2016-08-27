/*
 * journal_replayer.hpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_JOURNAL_REPLAYER_HPP_
#define JOURNAL_JOURNAL_REPLAYER_HPP_

#include <aio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <string>
#include <atomic>
#include <thread>

#include "../rpc/clients/replayer_client.hpp"
#include "../cache/cache_recover.hpp"

namespace Journal {

class JournalReplayer {
    //todo
public:
    JournalReplayer(int replay_interval,
            int update_interval,
            const std::string& rpc_addr,
            const std::string& vol_id,
            const std::string& device,
            std::shared_ptr<CacheProxy> cache_proxy_ptr,
            std::shared_ptr<IDGenerator> id_maker_ptr);
    void start_recover();
    void stop_recover();

    void start_replay();
    void stop_replay();
private:
    void replay_thread();
    void replay_volume();

    bool process_cache(int vol_fd, std::shared_ptr<ReplayEntry> r_entry);
    bool process_file(int vol_fd, const std::string& journal, off_t off);

    void update_maker_thread();
    void update_marker();

    bool get_journal_marker();
    bool get_journal_list(int limit);
    bool update_consumer_marker();

    int replay_interval;
    int update_interval;
    std::string vol_id;
    std::string device;
    std::atomic_bool stop_atomic;
    aiocb w_aiocb;

    JournalMarker journal_marker;
    std::list<std::string> journals;

    std::mutex entry_mutex;
    std::shared_ptr<CEntry> latest_entry;
    std::unique_ptr<std::thread> replay_thread_ptr;
    std::unique_ptr<std::thread> update_maker_thread_ptr;
    std::shared_ptr<ReplayerClient> rpc_client_ptr;
    std::unique_ptr<CacheRecovery> cache_recover_ptr;
    std::shared_ptr<CacheProxy> cache_proxy_ptr;
    std::shared_ptr<IDGenerator> id_maker_ptr;
};

}

#endif /* JOURNAL_JOURNAL_REPLAYER_HPP_ */
