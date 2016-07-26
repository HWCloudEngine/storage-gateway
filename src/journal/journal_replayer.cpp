/*
 * journal_replayer.cpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#include "journal_replayer.hpp"

namespace Journal {

JournalReplayer::JournalReplayer() :
        replay_thread_ptr() {
    //todo
    stop_atomic = false;
}

void JournalReplayer::replay(const std::string& vol_id, int interval_time) {
    //todo
    while (!stop_atomic.load()) {
        //todo: replay vol
        std::this_thread::sleep_for(std::chrono::seconds(interval_time));
    }
}

void JournalReplayer::start_replay(const std::string& vol_id,
        int interval_time) {
    replay_thread_ptr.reset(
            new std::thread(&JournalReplayer::replay, this, vol_id,
                    interval_time));
}

void JournalReplayer::stop_replay() {
    if (replay_thread_ptr.get()) {
        stop_atomic.store(true);
        replay_thread_ptr->join();
        replay_thread_ptr.reset();
    }
}

}
