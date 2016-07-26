/*
 * replay_manager.cpp
 *
 *  Created on: 2016Äê7ÔÂ28ÈÕ
 *      Author: smile-luobin
 */

#include "replay_manager.hpp"

namespace Journal {

ReplayManager::ReplayManager(int interval_time) :
        interval_time(interval_time) {
    init();
    start_replay();
}

void ReplayManager::init(){
    //todo: init vol_replayer_map from conf
}

void ReplayManager::start_replay() {
    for (auto vol_replayer : vol_replayer_map) {
        std::shared_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer());
        vol_replayer.second = journal_replayer_ptr;
        journal_replayer_ptr->start_replay(vol_replayer.first, interval_time);
    }
}

bool ReplayManager::add_vol_replayer(const std::string& vol_id) {
    std::lock_guard<std::mutex> lg(map_mutex);
    auto pos=vol_replayer_map.find(vol_id);
    if (pos == vol_replayer_map.end()) {
        std::shared_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer());
        vol_replayer_map[vol_id] = journal_replayer_ptr;
        journal_replayer_ptr->start_replay(vol_id, interval_time);
        return true;
    }else{
        return false;
    }
}

void ReplayManager::remove_vol_replayer(const std::string& vol_id) {
    std::lock_guard<std::mutex> lg(map_mutex);
    auto pos=vol_replayer_map.find(vol_id);
    if(pos != vol_replayer_map.end()){
        pos->second->stop_replay();
        vol_replayer_map.erase(pos);
    }
}

}
