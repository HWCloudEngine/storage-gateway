/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   timer.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: timer task
*
*************************************************/
#ifndef SRC_COMMON_TIMER_H_
#define SRC_COMMON_TIMER_H_
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include "log/log.h"

class TimerTask {
 public:
     TimerTask();
     virtual ~TimerTask();
     
     void start(uint64_t delay, uint64_t interval);
     void stop();

     static void timer_hanlder(union sigval sv);
 private:
     virtual void callback() = 0;
     
     struct itimerspec its_;
     struct sigevent sev_;
     timer_t timer_;
};

#endif  // SRC_COMMON_TIMER_H_
