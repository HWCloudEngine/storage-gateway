/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   perf_counter.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: object storage for block data 
*
*************************************************/
#ifndef SRC_COMMON_PERF_COUNTER_H_
#define SRC_COMMON_PERF_COUNTER_H_
#include <mutex>
#include <map>
#include <atomic>
#include <thread>
#include "message.h"

using namespace Journal;

#define ENABLE_PERF

typedef struct {
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
    uint64_t read_begin_ts;
    uint64_t read_end_ts;
    uint64_t reply_begin_ts;
    uint64_t reply_end_ts;
} IoProbe;

class PerfCounter {
 private:
     PerfCounter(); 
     ~PerfCounter();
 public:
    static PerfCounter& instance();
    void insert(uint64_t seq, IoProbe* probe);
    IoProbe* retrieve(uint64_t seq);
    void remove(uint64_t seq);

    uint64_t interval(uint64_t start, uint64_t end);
    void show(IoProbe* probe);    
    void report();
 private:
    std::map<uint64_t, IoProbe*> probes_;
    std::mutex lock_;
    std::atomic_bool run_;
    std::thread* thr_;
};

#define g_perf (PerfCounter::instance())

typedef enum {
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
} perf_phase_t;

#ifdef ENABLE_PERF
#define PRE_PERF(seq, dir, off, len) do {                     \
    IoProbe* probe = (IoProbe*)malloc(sizeof(IoProbe));       \
    if (probe) {                                              \
        probe->seq = seq;                                     \
        probe->dir = dir;                                     \
        probe->off = off;                                     \
        probe->len = len;                                     \
        probe->recv_begin_ts = Env::instance()->now_micros(); \
        g_perf.insert(probe->seq, probe);                     \
    }                                                         \
} while(0)

#define DO_PERF(phase, seq) do {             \
    IoProbe* probe = g_perf.retrieve(seq);   \
    if (probe) {                             \
        switch (phase) {                 \
        case RECV_BEGIN:                 \
            probe->recv_begin_ts = Env::instance()->now_micros(); \
            break;                       \
        case RECV_END:                   \
            probe->recv_end_ts = Env::instance()->now_micros(); \
            break;                       \
        case PROC_BEGIN:                 \
            probe->proc_begin_ts = Env::instance()->now_micros(); \
            break;                       \
        case PROC_END:                   \
            probe->proc_end_ts = Env::instance()->now_micros(); \
            break;                       \
        case WRITE_BEGIN:                \
            probe->write_begin_ts = Env::instance()->now_micros(); \
            break;                       \
        case WRITE_END:                  \
            probe->write_end_ts = Env::instance()->now_micros(); \
            break;                       \
        case READ_BEGIN:                 \
            probe->read_begin_ts = Env::instance()->now_micros(); \
            break;                       \
        case READ_END:                   \
            probe->read_end_ts = Env::instance()->now_micros(); \
            break;                       \
        case REPLY_BEGIN:                \
            probe->reply_begin_ts = Env::instance()->now_micros(); \
            break;                       \
        case REPLY_END:                  \
            probe->reply_end_ts = Env::instance()->now_micros(); \
            break;                       \
        default:                         \
            break;                       \
        }                                \
    }                                    \
} while(0)

#define POST_PERF(seq) do {                \
    IoProbe* probe = g_perf.retrieve(seq); \
    if (probe) {                           \
        g_perf.show(probe);                \
        g_perf.remove(seq);                \
    }                                      \
} while(0)
#else
#define PRE_PERF(seq, dir, off, len) ()
#define DO_PERF(phase, seq) ()
#define POST_PERF(seq) ()
#endif

#endif  //  SRC_COMMON_PERF_COUNTER_H_
