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
    recover_cache();
    start_replay();
}

void ReplayManager::init() {
    //todo: init vol_device_map and vol_replayer_map from conf
}

//recover cache
void ReplayManager::recover_cache() {
    for (auto vol_replayer_pair : vol_replayer_map) {
        std::string vol_id = vol_replayer_pair.first;

        //init cache proxy
        std::shared_ptr<CacheProxy> cache_proxy_ptr(new CacheProxy(vol_id));
        vol_proxy_map[vol_id] = cache_proxy_ptr;

        //init journal_replayer
        std::unique_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer(rpc_addr, vol_replayer_pair.first,
                        vol_device_map[vol_id]), cache_proxy_ptr);
        vol_replayer_pair.second = journal_replayer_ptr;

        //start recover
        journal_replayer_ptr->recover_cache();
    }
    for(auto vol_replayer_pair: vol_replayer_map){
        //stop recover
        vol_replayer_pair.second->stop_recover();
    }
}

//replay journal
void ReplayManager::start_replay() {
    for (auto vol_replayer_pair : vol_replayer_map) {
        journal_replayer_ptr->start_replay(interval_time);
    }
}

bool ReplayManager::add_vol_replayer(const std::string& vol_id,
        const std::string& device) {
    std::lock_guard<std::mutex> lg(map_mutex);
    auto pos = vol_replayer_map.find(vol_id);
    if (pos == vol_replayer_map.end()) {
        //init cache proxy
        std::shared_ptr<CacheProxy> cache_proxy_ptr(new CacheProxy(vol_id));
        vol_proxy_map[vol_id] = cache_proxy_ptr;

        //init journal replayer
        std::unique_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer(rpc_addr, vol_id, device));
        vol_replayer_map[vol_id] = journal_replayer_ptr;

        vol_device_map[vol_id] = deivce;

        //start replay
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
        vol_device_map.erase(vol_id);
        vol_proxy_map.erase(vol_id);
        return true;
    } else {
        return false;
    }
}

//get cache proxy
std::shared_ptr<CacheProxy> ReplayManager::get_cache_proxy_ptr(
        const std::string& vol_id) {
    auto pos = vol_proxy_map.find(vol_id);
    if (pos != vol_proxy_map.end()) {
        return vol_proxy_map[vol_id];
    } else {
        return nullptr;
    }
}

}
