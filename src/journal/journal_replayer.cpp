/*
 * journal_replayer.cpp
 *
 *  Created on: 2016Äê7ÔÂ14ÈÕ
 *      Author: smile-luobin
 */

#include <cstdio>
#include <iostream>
#include <errno.h>
#include "journal_replayer.hpp"

namespace Journal {

JournalReplayer::JournalReplayer(int replay_interval,
        int update_interval,
        const std::string& rpc_addr,
        const std::string& vol_id, const std::string& device,
        std::shared_ptr<CacheProxy> cache_proxy_ptr,
        std::shared_ptr<IDGenerator> id_maker_ptr) :
        replay_interval(replay_interval),
        update_interval(update_interval),
        vol_id(vol_id),
        device(device),
        w_aiocb(),
        journal_marker(),
        cache_proxy_ptr(cache_proxy_ptr),
        id_maker_ptr(id_maker_ptr) {
    bzero(&w_aiocb, sizeof(w_aiocb));
    rpc_client_ptr.reset(
            new ReplayerClient(
                    grpc::CreateChannel(rpc_addr,
                            grpc::InsecureChannelCredentials())));
    stop_atomic = false;
    latest_entry.reset();
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
            new std::thread(&JournalReplayer::replay_thread, this));
    update_maker_thread_ptr.reset(
            new std::thread(&JournalReplayer::update_maker_thread, this));
}

//stop replay volume
void JournalReplayer::stop_replay() {
    if (replay_thread_ptr.get()) {
        stop_atomic.store(true);
        replay_thread_ptr->join();
        replay_thread_ptr.reset();
    }
}

void JournalReplayer::replay_thread() {
    while (!stop_atomic.load()) {
        replay_volume();
        std::this_thread::sleep_for(std::chrono::seconds(replay_interval));
    }
}

void JournalReplayer::update_maker_thread(){
    while(!stop_atomic.load()){
        std::this_thread::sleep_for(std::chrono::seconds(update_interval));
        update_marker();
    }
}

//update marker
void JournalReplayer::update_marker(){
    if(latest_entry.get()){
        std::lock_guard<std::mutex> lg(entry_mutex);
        std::string file_name = latest_entry->get_log_file();
        off_t off = latest_entry->get_log_offset();
        journal_marker.set_cur_journal(file_name.c_str());
        journal_marker.set_pos(off);
        update_consumer_marker();
    }
}

//replay volume
void JournalReplayer::replay_volume() {
    //wait the last journal replay finished
    if (latest_entry.get()) {
        while (aio_error(&w_aiocb) == EINPROGRESS);
        if (aio_return(&w_aiocb) == 0) {
            std::cout << "last replay succeed" << std::endl;
            cache_proxy_ptr->reclaim(latest_entry);
        }
    }

    //replay new journal
    bzero(&w_aiocb, sizeof(w_aiocb));
    int vol_fd = open(device.c_str(), O_WRONLY);
    if (vol_fd < 0) {
        std::cout << "open volume failed" << std::endl;
        return false;
    }

    //todo: config replay_number(current only replay one journal entry)
    Jkey top = cache_proxy_ptr->top();
    if (nullptr != top) {
        std::shared_ptr<CEntry> entry = cache_proxy_ptr->retrieve(top);
        if (entry->cache_type == entry->IN_MEM) {
            //replay from memory
            bool succeed = process_cache(vol_fd, entry->get_log_entry());
            if (true == succeed) {
                std::lock_guard<std::mutex> lg(entry_mutex);
                latest_entry = entry;
            }
        } else {
            //todo: replay from journal file
            const std::string file_name = entry->get_log_file();
            const off_t off = entry->get_log_offset();
            bool succeed = process_file(vol_fd, file_name, off);
            if (true == succeed) {
                std::lock_guard<std::mutex> lg(entry_mutex);
                latest_entry = entry;
            }
        }
    } else {
        std::cout << "no jcache" << std::endl;
    }
    close(vol_fd);
}

bool JournalReplayer::process_cache(int vol_fd,
        std::shared_ptr<ReplayEntry> r_entry) {
    /*get header*/
    log_header_t* log_head = r_entry->header();
    off_t header_length = r_entry->header_lenth();

    /*get off len*/
    off_t off_len_start = sizeof(log_header_t);
    off_t body_start = 0;
    while (off_len_start != header_length) {
        off_len_t* off_len = reinterpret_cast<off_len_t *>(r_entry->data()
                + off_len_start);
        uint64_t off = off_len->offset;
        uint32_t length = off_len->length;
        char* data = r_entry->body() + body_start;

        //todo: check crc

        //wait the latest data replay finished
        while (aio_error(&w_aiocb) == EINPROGRESS);
        if (aio_return(&w_aiocb) != 0) {
            std::cout << "latest data replay failed" << std::endl;
            return false;
        }

        //replay current data
        bzero(&w_aiocb, sizeof(w_aiocb));
        w_aiocb.aio_fildes = vol_fd;
        w_aiocb.aio_buf = data;
        w_aiocb.aio_offset = off;
        w_aiocb.aio_nbytes = length;
        aio_write(&w_aiocb);

        off_len_start += sizeof(off_len_t);
        body_start += length;
    }
    return true;
}

bool JournalReplayer::process_file(int vol_fd, const std::string& file_name,
        off_t off) {
    int src_fd = open(file_name.c_str(), O_RDONLY);
    if (src_fd < 0) {
        std::cout << "open journal file failed" << std::endl;
        return false;
    }

    /*read header*/
    lseek(src_fd, off, SEEK_CUR);
    log_header_t log_head;
    memset(&log_head, 0, sizeof(log_head));
    size_t head_size = sizeof(log_head);
    int ret = read(src_fd, &log_head, head_size);
    if (ret != head_size) {
        std::cout << "read log head failed ret=" << ret << std::endl;
        return false;
    }

    /*read off len */
    size_t off_len_size = log_head.count * sizeof(off_len_t);
    off_len_t* off_len = (off_len_t*) malloc(off_len_size);
    ret = read(src_fd, off_len, off_len_size);
    if (ret != off_len_size) {
        std::cout << "read log off len failed ret=" << ret << std::endl;
        return false;
    }

    /*read data and replay data*/
    for (int i = 0; i < log_head.count; i++) {
        uint64_t off = log_head.off_len[i].offset;
        uint32_t length = log_head.off_len[i].length;
        char* data = (char*) malloc(sizeof(char) * length);
        ret = read(src_fd, data, length);
        if (ret != length) {
            std::cout << "read data failed ret=" << ret << std::endl;
            return false;
        }

        //todo: check crc

        //wait the latest data replay finished
        while (aio_error(&w_aiocb) == EINPROGRESS);
        if (aio_return(&w_aiocb) != 0) {
            std::cout << "latest data replay failed" << std::endl;
            return false;
        }

        //replay current data
        bzero(&w_aiocb, sizeof(w_aiocb));
        w_aiocb.aio_fildes = vol_fd;
        w_aiocb.aio_buf = data;
        w_aiocb.aio_offset = off;
        w_aiocb.aio_nbytes = length;
        aio_write(&w_aiocb);
    }
    close(src_fd);
    return true;
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
