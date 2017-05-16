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

#endif  //  SRC_COMMON_PERF_COUNTER_H_
