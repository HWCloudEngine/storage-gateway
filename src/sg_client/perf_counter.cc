/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   perf_counter.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: perf stat
*
*************************************************/
#include <unistd.h>
#include <pthread.h>
#include "log/log.h"
#include "message.h"
#include "perf_counter.h"

PerfCounter::PerfCounter() {
    perf_run_ = true;
    perf_thr_ = new std::thread(std::bind(&PerfCounter::perf_work, this));
}

PerfCounter::~PerfCounter() {
    probe_map_.clear();
    perf_run_ = false;
    if (perf_thr_) {
        perf_thr_->join();
        delete perf_thr_;
    }
}

static PerfCounter* default_perf = nullptr;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init_default_perf() {
    default_perf = new PerfCounter();
}

PerfCounter& PerfCounter::instance() {
    pthread_once(&once, init_default_perf);
    return *default_perf;
}

void PerfCounter::start_perf(uint64_t seq, uint8_t dir, uint64_t off, uint64_t len) {
#ifdef ENABLE_PERF
    std::lock_guard<std::mutex> lock(probe_map_lock_);
    io_probe_t probe = {0};
    probe.seq = (seq);
    probe.dir = (dir);
    probe.off = (off);
    probe.len = (len);
    g_perf.insert(seq, probe);
#endif
}

void PerfCounter::doing_perf(perf_phase_t phase, uint64_t seq) {
#ifdef ENABLE_PERF
    std::lock_guard<std::mutex> lock(probe_map_lock_);
    io_probe_t* probe = nullptr;
    g_perf.fetch(seq, &probe);
    if (probe == nullptr) {
        return;
    }
    uint64_t now_micros = Env::instance()->now_micros();
    switch (phase) {
        case RECV_BEGIN:
            probe->recv_begin_ts = now_micros;
            break;
        case RECV_END:
            probe->recv_end_ts = now_micros;
            break;
        case PROC_BEGIN:
            probe->proc_begin_ts = now_micros;
            break;
        case PROC_END:
            probe->proc_end_ts = now_micros;
            break;
        case WRITE_BEGIN:
            probe->write_begin_ts = now_micros;
            break;
        case WRITE_END:
            probe->write_end_ts = now_micros;
            break;
        case READ_BEGIN:
            probe->read_begin_ts = now_micros;
            break;
        case READ_END:
            probe->read_end_ts = now_micros;
            break;
        case REPLY_BEGIN:
            probe->reply_begin_ts = now_micros;
            break;
        case REPLY_END:
            probe->reply_end_ts = now_micros;
            break;
        case REPLAY_BEGIN:
            probe->replay_begin_ts = now_micros;
            break;
        case REPLAY_END:
            probe->replay_end_ts = now_micros;
            break;
        default:
            break;
    }
#endif
}

void PerfCounter::done_perf(uint64_t seq) {

}

void PerfCounter::insert(uint64_t seq, io_probe_t  probe) {
    auto it = probe_map_.find(seq);
    if (it != probe_map_.end()) {
        probe_map_.erase(seq);
    }
    probe_map_.insert({seq, probe}); 
}

void PerfCounter::remove(uint64_t seq) {
    auto it = probe_map_.find(seq);
    if (it == probe_map_.end()) {
        return;
    }
    probe_map_.erase(seq);
}

void PerfCounter::fetch(uint64_t seq, io_probe_t** probe) {
    auto it = probe_map_.find(seq);
    if (it == probe_map_.end()) {
        *probe = nullptr;
        return;
    }
    *probe = &(it->second);
}

uint64_t PerfCounter::cost_time(uint64_t start, uint64_t end) {
    return (end - start);
}

void PerfCounter::show_stat(io_request_code_t rw) {
    uint64_t total_io_time = 0;
    uint64_t total_io_num = 0;
    uint64_t total_io_bytes = 0;
    uint64_t max = 0;
    uint64_t min = UINTMAX_MAX;
    uint64_t max_io_seq = 0;
    uint64_t min_io_seq = 0;
    for (auto it : probe_map_) {
        if (it.second.dir != (uint8_t)rw) {
            continue;
        }
        uint64_t time = cost_time(it.second.recv_begin_ts, it.second.reply_end_ts);
        if (time > max) {
            max = time;
            max_io_seq = it.first;
        }
        if (time < min) {
            min = time; 
            min_io_seq = it.first;
        }
        total_io_time += time;
        total_io_bytes += it.second.len;
        total_io_num++;
    }
    
    if (total_io_num) {
        LOG_INFO << "rw:" << rw << " total_io_num:" << total_io_num
                 << " total_io_bytes:" << total_io_bytes
                 << " total_io_time:"  << total_io_time
                 << " max:" << max << " max_io_seq:" << max_io_seq
                 << " min:" << min << " min_io_seq:" << min_io_seq;
    }
}

void PerfCounter::show_probe(const io_probe_t* probe) {
    if (probe->dir == SCSI_READ) {
        LOG_INFO << "seq:" << probe->seq
                 << " dir:" << (io_request_code_t)probe->dir
                 << " off:" << probe->off
                 << " len:" << probe->len
                 << " recv:" << cost_time(probe->recv_begin_ts, probe->recv_end_ts)
                 << " read:" << cost_time(probe->read_begin_ts, probe->read_end_ts)
                 << " reply:" << cost_time(probe->reply_begin_ts, probe->reply_end_ts);
    } else if (probe->dir == SCSI_WRITE) {
        LOG_INFO << "seq:" << probe->seq
                 << " dir:" << (io_request_code_t)probe->dir
                 << " off:" << probe->off
                 << " len:" << probe->len
                 << " recv:" << cost_time(probe->recv_begin_ts, probe->recv_end_ts)
                 << " proc:" << cost_time(probe->proc_begin_ts, probe->proc_end_ts)
                 << " write:" << cost_time(probe->write_begin_ts, probe->write_end_ts)
                 << " reply:" << cost_time(probe->reply_begin_ts, probe->reply_end_ts)
                 << " replay:" << cost_time(probe->replay_begin_ts, probe->replay_end_ts);
    }
}

void PerfCounter::perf_work() {
    while (perf_run_) {
        {
            std::lock_guard<std::mutex> lock(probe_map_lock_);
            for (auto it : probe_map_){
                show_probe(&(it.second));
            }
            show_stat(SCSI_READ);
            show_stat(SCSI_WRITE);
            probe_map_.clear();
        }
       sleep(5);
    }
}
