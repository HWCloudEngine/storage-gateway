/*
 * replay_manager.cpp
 *
 *  Created on: 2016Äê7ÔÂ28ÈÕ
 *      Author: smile-luobin
 */

#include "replay_manager.hpp"

namespace Journal {

ReplayManager::ReplayManager(const std::string& rpc_addr, int interval_time) :
        interval_time(interval_time), rpc_addr(rpc_addr) {
    init();
    init_cache();
    start_replay();
}

void ReplayManager::init() {
    //todo: init vol_replayer_map from conf
}

void ReplayManager::init_cache() {
    for (auto vol_replayer_pair : vol_replayer_map) {
        std::shared_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer(rpc_addr, vol_replayer_pair.first));
        vol_replayer_pair.second = journal_replayer_ptr;
        journal_replayer_ptr->init_cache();
    }
}

void ReplayManager::start_replay() {
    for (auto vol_replayer_pair : vol_replayer_map) {
        journal_replayer_ptr->start_replay(interval_time);
    }
}

bool ReplayManager::add_vol_replayer(const std::string& vol_id) {
    std::lock_guard<std::mutex> lg(map_mutex);
    auto pos = vol_replayer_map.find(vol_id);
    if (pos == vol_replayer_map.end()) {
        std::shared_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer(rpc_addr, pos->first));
        vol_replayer_map[vol_id] = journal_replayer_ptr;
        journal_replayer_ptr->start_replay(vol_id, interval_time);
        return true;
    } else {
        return false;
    }
}

bool ReplayManager::remove_vol_replayer(const std::string& vol_id) {
    std::lock_guard<std::mutex> lg(map_mutex);
    auto pos = vol_replayer_map.find(vol_id);
    if (pos != vol_replayer_map.end()) {
        pos->second->stop_replay();
        vol_replayer_map.erase(pos);
        return true;
    } else {
        return false;
    }
}

}
