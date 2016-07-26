/*
 * replay_manager.hpp
 *
 *  Created on: 2016Äê7ÔÂ28ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_REPLAY_MANAGER_HPP_
#define JOURNAL_REPLAY_MANAGER_HPP_

#include <map>
#include <mutex>
#include <string>
#include "journal_replayer.hpp"

namespace Journal {

class ReplayManager {
public:
    ReplayManager(const std::string& rpc_addr, int interval_time);
    bool add_vol_replayer(const std::string& vol_id);
    bool remove_vol_replayer(const std::string& vol_id);
private:
    void start_replay();
    void init_cache();
    void init();

    int interval_time;
    std::string rpc_addr;
    std::mutex map_mutex;
    std::map<std::string, std::shared_ptr<JournalReplayer>> vol_replayer_map;
};

}

#endif /* JOURNAL_REPLAY_MANAGER_HPP_ */
