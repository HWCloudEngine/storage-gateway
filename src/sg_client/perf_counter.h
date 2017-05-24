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
#include <map>
#include "common/timer.h"
#include "common/atomic_ptr.h"
#include "common/env_posix.h"
#include "message.h"

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

using probe_map_t = std::map<uint64_t, IoProbe>;

class PerfCounter : public TimerTask {
 public:
     PerfCounter(); 
     ~PerfCounter();

 public:
    static PerfCounter& instance();

    void     insert(uint64_t seq, IoProbe probe);
    IoProbe* fetch(uint64_t seq);
    void     remove(uint64_t seq);

    void show_probe(const IoProbe* probe);    

 private:
    probe_map_t* native_map();
    uint64_t cost_time(uint64_t start, uint64_t end);
    void show_stat(io_request_code_t rw);
    /*timer callback*/
    virtual void callback() override;

 private:
    AtomicPtr* probe_map_;
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
static inline void pre_perf(uint64_t seq, uint8_t dir, uint64_t off, uint64_t len) {
    IoProbe probe = {0};
    probe.seq = (seq);
    probe.dir = (dir);
    probe.off = (off);
    probe.len = (len);
    g_perf.insert(seq, probe);
}

static inline void do_perf(perf_phase_t phase, uint64_t seq) {
    IoProbe* probe = g_perf.fetch(seq);
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
        default:
            break;
    }
}

static inline void post_perf(uint64_t seq) {
    //IoProbe* probe = g_perf.fetch(seq);
    //if (probe) {
    //   g_perf.show_probe(probe);
    //}
}
#else
static inline void pre_perf(uint64_t seq, uint8_t dir, uint64_t off, uint64_t len) {}
static inline void do_perf(perf_phase_t phase, uint64_t seq) {}
static inline void post_perf(uint64_t seq) {}
#endif

#endif  //  SRC_COMMON_PERF_COUNTER_H_
