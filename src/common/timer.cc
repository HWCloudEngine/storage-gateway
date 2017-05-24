/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   timer.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: timer task
*
*************************************************/
#include "log/log.h"
#include "timer.h"

TimerTask::TimerTask() {
    sev_.sigev_notify = SIGEV_THREAD;
    sev_.sigev_notify_function = timer_hanlder;
    sev_.sigev_notify_attributes = NULL;
    sev_.sigev_value.sival_ptr = this;
    
    if (timer_create(CLOCK_REALTIME, &sev_, &timer_) == -1) {
        LOG_ERROR << "create timer failed";
    }
}


TimerTask::~TimerTask() {
    timer_delete(timer_);
}

void TimerTask::start(uint64_t delay, uint64_t interval) {
    its_.it_value.tv_sec = delay;
    its_.it_value.tv_nsec = 0;
    its_.it_interval.tv_sec = interval;
    its_.it_interval.tv_nsec = 0;
    if (timer_settime(timer_, 0, &its_, NULL) == -1) {
        LOG_ERROR << "timer settime failed";
    }
}

void TimerTask::stop() {
    timer_delete(timer_);
}

void TimerTask::timer_hanlder(union sigval sv) {
    reinterpret_cast<TimerTask*>(sv.sival_ptr)->callback();
}
