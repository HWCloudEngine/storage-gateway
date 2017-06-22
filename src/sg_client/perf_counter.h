/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   perf_counter.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: perf stat
*
*************************************************/
#ifndef SRC_COMMON_PERF_COUNTER_H_
#define SRC_COMMON_PERF_COUNTER_H_
#include <mutex>
#include <atomic>
#include <map>
#include <thread>
#include "common/timer.h"
#include "common/env_posix.h"
#include "message.h"

//#define ENABLE_PERF

struct io_probe {
    uint64_t seq;
    uint8_t  dir;
    off_t    off;
    size_t   len;
    uint64_t recv_begin_ts;
    uint64_t recv_end_ts;
    uint64_t proc_begin_ts;
    uint64_t proc_end_ts;
    uint64_t write_begin_ts;
    uint64_t write_end_ts;
    uint64_t replay_begin_ts;
    uint64_t replay_end_ts;
    uint64_t read_begin_ts;
    uint64_t read_end_ts;
    uint64_t reply_begin_ts;
    uint64_t reply_end_ts;
};
typedef struct io_probe io_probe_t;

enum perf_phase {
    RECV_BEGIN  = 0,
    RECV_END    = 1,
    PROC_BEGIN  = 2,
    PROC_END    = 3,
    WRITE_BEGIN = 4,
    WRITE_END   = 5,
    READ_BEGIN  = 6,
    READ_END    = 7,
    REPLY_BEGIN = 8,
    REPLY_END   = 9,
    REPLAY_BEGIN = 10,
    REPLAY_END   = 11,
};
typedef perf_phase perf_phase_t;

class PerfCounter {
 public:
     PerfCounter(); 
     PerfCounter(const PerfCounter& other) = delete;
     PerfCounter& operator=(const PerfCounter& other) = delete;
     ~PerfCounter();

 public:
    static PerfCounter& instance();

    void start_perf(uint64_t seq, uint8_t dir, uint64_t off, uint64_t len);
    void doing_perf(perf_phase_t phase, uint64_t seq);
    void done_perf(uint64_t seq);

 private:
    void insert(uint64_t seq, io_probe_t probe);
    void fetch(uint64_t seq, io_probe_t** probe);
    void remove(uint64_t seq);
    void show_probe(const io_probe_t* probe);    
    uint64_t cost_time(uint64_t start, uint64_t end);
    void show_stat(io_request_code_t rw);
    void perf_work();

 private:
    std::mutex probe_map_lock_;
    std::map<uint64_t, io_probe_t> probe_map_;
    std::atomic_bool perf_run_;
    std::thread* perf_thr_;
};

#define g_perf (PerfCounter::instance())

#define pre_perf(seq, dir, off, len) do { \
    g_perf.start_perf(seq, dir, off, len); \
} while(0)

#define do_perf(phase, seq) do { \
    g_perf.doing_perf(phase, seq); \
} while(0)

#define post_perf(seq) do { \
    g_perf.done_perf(seq); \
} while(0)

#endif  //  SRC_COMMON_PERF_COUNTER_H_
