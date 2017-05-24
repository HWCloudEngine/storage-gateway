/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   perf_counter.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: object storage for block data 
*
*************************************************/
#include <unistd.h>
#include <pthread.h>
#include "log/log.h"
#include "message.h"
#include "perf_counter.h"

PerfCounter::PerfCounter() {
    probe_map_ = new AtomicPtr(new probe_map_t);
}

PerfCounter::~PerfCounter() {
    if (native_map()) {
        native_map()->clear();
    }
}

static PerfCounter* default_perf = nullptr;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init_default_perf() {
    default_perf = new PerfCounter();
    default_perf->start(1, 10);
}

PerfCounter& PerfCounter::instance() {
    pthread_once(&once, init_default_perf);
    return *default_perf;
}

probe_map_t* PerfCounter::native_map() {
    return reinterpret_cast<probe_map_t*>(probe_map_->lock_load());
}

void PerfCounter::insert(uint64_t seq, IoProbe  probe) {
    auto it = native_map()->find(seq);
    if (it != native_map()->end()) {
        native_map()->erase(seq);
    }
    native_map()->insert({seq, probe}); 
}

void PerfCounter::remove(uint64_t seq) {
    auto it = native_map()->find(seq);
    if (it == native_map()->end()) {
        return;
    }
    native_map()->erase(seq);
}

IoProbe* PerfCounter::fetch(uint64_t seq) {
    auto it = native_map()->find(seq);
    if (it == native_map()->end()) {
        return nullptr;
    }
    return &(it->second);
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
    for (auto it : (*(native_map()))) {
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

    LOG_INFO << "rw:" << rw << " total_io_num:" << total_io_num
             << " total_io_bytes:" << total_io_bytes
             << " total_io_time:"  << total_io_time
             << " max:" << max << " max_io_seq:" << max_io_seq
             << " min:" << min << " min_io_seq:" << min_io_seq;
}

void PerfCounter::show_probe(const IoProbe* probe) {
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
                 << " reply:" << cost_time(probe->reply_begin_ts, probe->reply_end_ts);
    }
}

void PerfCounter::callback() {
    LOG_INFO << "report now ";
    for (auto it : (*(native_map()))) {
        show_probe(&(it.second));
    }
    show_stat(SCSI_READ);
    show_stat(SCSI_WRITE);
    native_map()->clear();
    LOG_INFO << "report now ok ";
}
