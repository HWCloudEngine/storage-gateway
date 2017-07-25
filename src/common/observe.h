/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   observe.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: observe interface
*
*************************************************/
#ifndef SRC_COMMON_OBSERVE_H_
#define SRC_COMMON_OBSERVE_H_
#include <vector>

class Observee {
 public:
     Observee();
     virtual ~Observee();

     virtual void update(int event, void* arg) = 0;
};

class Observer {
 public:
     Observer();
     virtual ~Observer();

     void add_observee(Observee* obs);
     void del_observee(Observee* obs);

     virtual void notify(int event, void* arg) = 0;
 
 protected:
     std::vector<Observee*> obs_;
};

#endif   // SRC_COMMON_OBSERVE_H_
