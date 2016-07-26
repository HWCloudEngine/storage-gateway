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

namespace Journal {

class JournalReplayer {
    //todo
public:
    JournalReplayer();
    void start_replay(const std::string& vol_id, int interval_time);
    void stop_replay();
private:
    std::atomic_bool stop_atomic;
    std::shared_ptr<std::thread> replay_thread_ptr;
    void replay(const std::string& vol_id, int interval_time);
};

}

#endif /* JOURNAL_JOURNAL_REPLAYER_HPP_ */
