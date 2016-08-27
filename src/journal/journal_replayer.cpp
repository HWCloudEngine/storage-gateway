/*
 * journal_replayer.cpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#include <cstdio>
#include <iostream>
#include "journal_replayer.hpp"

namespace Journal {

JournalReplayer::JournalReplayer(const std::string& rpc_addr,
        const std::string& vol_id, const std::string& device,
        std::shared_ptr<CacheProxy> cache_proxy_ptr) :
        vol_id(vol_id), device(device), journal_marker(),
        cache_proxy_ptr(cache_proxy_ptr) {
    rpc_client_ptr.reset(
            new ReplayerClient(
                    grpc::CreateChannel(rpc_addr,
                            grpc::InsecureChannelCredentials())));
    id_maker_ptr.reset(new IDGenerator());
    stop_atomic = false;
}

//start recover cache
void JournalReplayer::start_recover() {
    cache_recover_ptr.reset(
            new CacheRecovery(vol_id, rpc_client_ptr, id_maker_ptr));
    cache_recover_ptr->start();
}

//stop recover cache
void JournalReplayer::stop_recover() {
    if (cache_recover_ptr.get()) {
        cache_recover_ptr->stop();
    }
}

//start replay volume
void JournalReplayer::start_replay(int interval_time) {
    replay_thread_ptr.reset(
            new std::thread(&JournalReplayer::replay_thread, this, vol_id,
                    interval_time));
}

//stop replay volume
void JournalReplayer::stop_replay() {
    if (replay_thread_ptr.get()) {
        stop_atomic.store(true);
        replay_thread_ptr->join();
        replay_thread_ptr.reset();
    }
}

void JournalReplayer::replay_thread(int interval_time) {
    while (!stop_atomic.load()) {
        replay_volume();
        std::this_thread::sleep_for(std::chrono::seconds(interval_time));
    }
}

//replay volume
void JournalReplayer::replay_volume() {
    //todo: config replay_number(current only replay one journal)
    if (false == replay_cache()) {
        replay_journal_file();
    }
}

//replay from cache
bool JournalReplayer::replay_cache() {
    //todo: config replay_number(current only replay one journal)
    Jkey top = cache_proxy_ptr->top();
    if (nullptr != top) {
        std::shared_ptr<CEntry> entry = cache_proxy_ptr->retrieve(top);
        if (entry->cache_type == entry->IN_MEM) {
            //todo: replay from memory
            FILE* vol_file_ptr = fopen(device.c_str(), "ab+");
            fwrite(entry->log_entry->data(), entry->log_offset,
                    entry->log_entry->length(), vol_file_ptr);
            fclose(vol_file_ptr);
            vol_file_ptr = nullptr;
        } else {
            if (false == process_file(entry->get_log_file())) {
                return false;
            }
        }
        cache_proxy_ptr->reclaim(entry);

        //update consumer marker
        journal_marker.set_cur_journal(entry->get_log_file());
        journal_marker.clear_pos();
        update_consumer_marker();

        std::cout << "replay from cache succeed" << std::endl;
        return true;
    } else {
        std::cout << "no jcache" << std::endl;
        return false;
    }
}

//replay from journal file
bool JournalReplayer::replay_journal_file() {
    if (nullptr != vol_file_ptr) {
        //todo:config limit in conf
        //todo: config replay_number(current only replay one journal)
        int limit = 1;
        if (get_journal_marker()) {
            if (get_journal_list(1)) {
                for (auto journal : journals) {
                    if (false == process_file(journal)) {
                        return false;
                    }
                }
                //update journal marker
                if (journals.size() < limit) {
                    journal_marker.clear_cur_journal();
                    journal_marker.clear_pos();
                } else {
                    journal_marker.set_cur_journal(*journals.rbegin());
                    journal_marker.clear_pos();
                }
                update_consumer_marker();
            }
        }
    }
    return true;
}

bool JournalReplayer::process_file(const std::string& file_name) {
    FILE* vol_file_ptr = fopen(device.c_str(), "ab+");
    if (nullptr != vol_file_ptr) {
        FILE* src_file_ptr = fopen(file_name.c_str(), "r");
        if (nullptr != src_file_ptr) {
            /*read header*/
            log_header_t log_head;
            memset(&log_head, 0, sizeof(log_head));
            size_t head_size = sizeof(log_head);
            int ret = fread((void*) &log_head, sizeof(char), head_size,
                    src_file_ptr);
            if (ret != sizeof(log_head)) {
                std::cout << "read log head failed ret=" << ret << std::endl;
                return false;
            }

            /*read off len */
            size_t off_len_size = log_head.count * sizeof(off_len_t);
            off_len_t* off_len = (off_len_t*) malloc(off_len_size);
            ret = fread((void*) off_len, sizeof(char), off_len_size,
                    src_file_ptr);
            if (ret != off_len_size) {
                std::cout << "read log off len failed ret=" << ret << std::endl;
                return false;
            }

            /*read data*/
            for (int i = 0; i < log_head.count; i++) {
                uint64_t off = log_head.off_len[i].offset;
                uint32_t length = log_head.off_len[i].length;
                char* data = (char*) malloc(sizeof(char) * length);
                ret = fread((void*) data, sizeof(char), length, src_file_ptr);
                if (ret != length) {
                    std::cout << "read data failed ret=" << ret << std::endl;
                    return false;
                }
                ret = fwrite(data, off, length, vol_file_ptr);
                if (ret != length) {
                    std::cout << "replay data failed ret=" << ret << std::endl;
                    return false;
                }
            }
            std::cout << "replay from journal file succeed" << std::endl;
            return true;
        } else {
            std::cout << "open journal file failed" << std::endl;
            return false;
        }
    } else {
        std::cout << "open volume file failed" << std::endl;
        return false;
    }
}

bool JournalReplayer::get_journal_marker() {
    if (rpc_client_ptr->GetJournalMarker(vol_id, journal_marker)) {
        return true;
    } else {
        return false;
    }
}

bool JournalReplayer::get_journal_list(int limit) {
    if (rpc_client_ptr->GetJournalList(vol_id, journal_marker, limit,
            journals)) {
        return true;
    } else {
        return false;
    }
}

bool JournalReplayer::update_consumer_marker() {
    if (rpc_client_ptr->UpdateConsumerMarker(journal_marker, vol_id)) {
        return true;
    } else {
        return false;
    }
}

}
