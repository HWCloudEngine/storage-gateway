/*
 * replay_manager.hpp
 *
 *  Created on: 2016Äê7ÔÂ28ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_REPLAY_MANAGER_HPP_
#define JOURNAL_REPLAY_MANAGER_HPP_

#include <list>
#include <string>
#include "journal_replayer.hpp"

namespace Journal {

class ReplayManager {
public:
    explicit ReplayManager(int interval_time);
    void add_vol(const std::string& vol_id);
    void remove_vol(const std::string& vol_id);
    void start_replay();
private:
    std::list<std::string> vol_list;
    std::list<std::shared_ptr<JournalReplayer>> replayer_ptr_list;
    int interval_time;
};

}

#endif /* JOURNAL_REPLAY_MANAGER_HPP_ */
