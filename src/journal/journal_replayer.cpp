/*
 * journal_replayer.cpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#include <iostream>
#include "journal_replayer.hpp"

namespace Journal {

JournalReplayer::JournalReplayer(const std::string& rpc_addr,
        const std::string& vol_id) :
        vol_id(vol_id),
        rpc_client(grpc::CreateChannel(rpc_addr,
                grpc::InsecureChannelCredentials())),
        journal_marker() {
    stop_atomic = false;
}

void JournalReplayer::replay(int interval_time) {
    while (!stop_atomic.load()) {
        replay_volume();
        std::this_thread::sleep_for(std::chrono::seconds(interval_time));
    }
}

void JournalReplayer::replay_volume() {
    //todo: replay volume
}

void JournalReplayer::start_replay(int interval_time) {
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

void JournalReplayer::init_cache() {
    auto init_thread = std::thread(&JournalReplayer::init_cache_thread, this, vol_id);
    init_thread.join();
}

void JournalReplayer::init_cache_thread() {
    //todo: init volume cache
}

bool JournalReplayer::get_journal_marker() {
    if(rpc_client.GetJournalMarker(vol_id, journal_marker)){
        return true;
    }else{
        return false;
    }
}

bool JournalReplayer::get_journal_list(int limit) {
    if(rpc_client.GetJournalList(vol_id, journal_marker, limit, journals)){
        return true;
    }else{
        return false;
    }
}

bool JournalReplayer::update_consumer_marker(){
    if(rpc_client.UpdateConsumerMarker(journal_marker, vol_id)){
        return true;
    }else{
        return false;
    }
}

}
