/*
 * replay_manager.cpp
 *
 *  Created on: 2016Äê7ÔÂ28ÈÕ
 *      Author: smile-luobin
 */

#include <algorithm>
#include "replay_manager.hpp"

namespace Journal {

ReplayManager::ReplayManager(int interval_time) :
        interval_time(interval_time) {
    //todo: init vol_list from conf
    this->start_replay();
}

void ReplayManager::start_replay() {
    for (auto vol_id : vol_list) {
        std::shared_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer());
        replayer_ptr_list.push_back(journal_replayer_ptr);
        journal_replayer_ptr->start_replay(vol_id, interval_time);
    }
}

void ReplayManager::add_vol(const std::string& vol_id) {
    std::list<std::string>::iterator pos;
    pos = find(vol_list.begin(), vol_list.end(), vol_id);
    if (pos == vol_list.end()) {
        vol_list.push_back(vol_id);
        std::shared_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer());
        replayer_ptr_list.push_back(journal_replayer_ptr);
        journal_replayer_ptr->start_replay(vol_id, interval_time);
    }
}

void ReplayManager::remove_vol(const std::string& vol_id) {
    std::list<std::string>::iterator vol_pos = vol_list.begin();
    std::list<std::shared_ptr<JournalReplayer>>::iterator replayer_ptr_pos =
            replayer_ptr_list.begin();
    for (; vol_pos != vol_list.end();) {
        if (vol_id.compare(*vol_pos) == 0) {
            vol_list.erase(vol_pos);
            (*replayer_ptr_pos)->stop_replay();
            replayer_ptr_list.erase(replayer_ptr_pos);
            break;
        } else {
            ++vol_pos;
            ++replayer_ptr_pos;
        }
    }
}

}
