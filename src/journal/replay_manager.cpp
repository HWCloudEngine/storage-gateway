/*
 * replay_manager.cpp
 *
 *  Created on: 2016Äê7ÔÂ28ÈÕ
 *      Author: smile-luobin
 */

#include "replay_manager.hpp"

namespace Journal {

ReplayManager::ReplayManager(int replay_interval,
        int update_interval,
        const std::string& rpc_addr,
        std::vector<std::string> vols,
        std::vector<std::string> devices,
        std::shared_ptr<IDGenerator> id_maker_ptr) :
            replay_interval(replay_interval),
            update_interval(update_interval),
            rpc_addr(rpc_addr),
            id_maker_ptr(id_maker_ptr){

    //init vol_*_map
    int vol_num = vols.size();
    for(int index = 0; index < vol_num; index++){
        std::string vol_id = vols[index];
        std::string device = devices[index];
        std::shared_ptr<CacheProxy> cache_proxy_ptr(new CacheProxy(vol_id));
        std::unique_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer(replay_interval, update_interval,
                        rpc_addr, vol_id, device,
                        cache_proxy_ptr, id_maker_ptr));

        vol_device_map[vol_id] = device;
        vol_proxy_map[vol_id] = cache_proxy_ptr;
        vol_replayer_map[vol_id] = journal_replayer_ptr;
    }

    //recover replay cache
    recover_cache();

    //start replay
    start_replay();
}


//recover cache
void ReplayManager::recover_cache() {
    //start recover cache
    for (auto vol_replayer_pair : vol_replayer_map) {
        vol_replayer_pair.second->start_recover();
    }

    //stop recover cache
    for (auto vol_replayer_pair : vol_replayer_map) {
        vol_replayer_pair.second->stop_recover();
    }
}

//replay journal
void ReplayManager::start_replay() {
    for (auto vol_replayer_pair : vol_replayer_map) {
        journal_replayer_ptr->start_replay();
    }
}

bool ReplayManager::add_vol_replayer(const std::string& vol_id,
        const std::string& device) {
    std::lock_guard<std::mutex> lg(map_mutex);
    auto pos = vol_replayer_map.find(vol_id);
    if (pos == vol_replayer_map.end()) {
        //init cache proxy
        std::shared_ptr<CacheProxy> cache_proxy_ptr(new CacheProxy(vol_id));
        //init journal replayer
        std::unique_ptr<JournalReplayer> journal_replayer_ptr(
                new JournalReplayer(replay_interval, update_interval,
                        rpc_addr, vol_id, device, cache_proxy_ptr,
                        id_maker_ptr));
        vol_replayer_map[vol_id] = journal_replayer_ptr;
        vol_proxy_map[vol_id] = cache_proxy_ptr;
        vol_device_map[vol_id] = deivce;

        //start replay
        journal_replayer_ptr->start_replay();
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
        vol_replayer_map.erase(vol_id);
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
