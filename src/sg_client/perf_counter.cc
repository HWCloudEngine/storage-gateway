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
#include "log/log.h"
#include "message.h"
#include "perf_counter.h"

PerfCounter::PerfCounter() {
    run_ = true;
    thr_ = new std::thread(std::bind(&PerfCounter::report, this));
}

PerfCounter::~PerfCounter() {
    if (thr_) {
        run_ = false;
        thr_->join();
        delete thr_;
    }
}

PerfCounter& PerfCounter::instance() {
    static PerfCounter perf;
    return perf;
}

void PerfCounter::insert(uint64_t seq, IoProbe* probe) {
    auto it = probes_.find(seq);
    if (it != probes_.end()) {
        IoProbe* old = it->second;
        if (old) {
            free(old);
        }
    }
    probes_.insert({seq, probe}); 
}

void PerfCounter::remove(uint64_t seq) {
    auto it = probes_.find(seq);
    if (it == probes_.end()) {
        return;
    }
    IoProbe* old = it->second;
    if (old) {
        free(old);
    }
    probes_.erase(seq);
}

IoProbe* PerfCounter::retrieve(uint64_t seq) {
    auto it = probes_.find(seq);
    if (it == probes_.end()) {
        return nullptr;
    }
    return it->second;
}

uint64_t PerfCounter::interval(uint64_t start, uint64_t end) {
    return (end - start);
}

void PerfCounter::show(IoProbe* probe) {
    if (probe->dir == SCSI_READ) {
        LOG_INFO << "seq:" << probe->seq << " dir:" << (IOHook_request_code_t)probe->dir
                 << " off:" << probe->off << " len:" << probe->len
                 << " recv:" << interval(probe->recv_begin_ts, probe->recv_end_ts)
                 << " read:" << interval(probe->read_begin_ts, probe->read_end_ts)
                 << " reply:" << interval(probe->reply_begin_ts, probe->reply_end_ts);
    } else if (probe->dir == SCSI_WRITE) {
        LOG_INFO << "seq:" << probe->seq << " dir:" << (IOHook_request_code_t)probe->dir
                 << " off:" << probe->off << " len:" << probe->len
                 << " recv:" << interval(probe->recv_begin_ts, probe->recv_end_ts)
                 << " proc:" << interval(probe->proc_begin_ts, probe->proc_end_ts)
                 << " write:" << interval(probe->write_begin_ts, probe->write_end_ts)
                 << " reply:" << interval(probe->reply_begin_ts, probe->reply_end_ts);
    }
}

void PerfCounter::report() {
    while (run_) {
        sleep(5); 
    }
}
