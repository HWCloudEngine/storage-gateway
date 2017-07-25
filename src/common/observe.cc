/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   observe.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: observe interface
*
*************************************************/
#include "observe.h"

Observee::Observee() {
}

Observee::~Observee() {
}

Observer::Observer() {
}

Observer::~Observer() {
}

void Observer::add_observee(Observee* obs) {
    obs_.push_back(obs);
}

void Observer::del_observee(Observee* obs) {
    for (auto it = obs_.begin(); it != obs_.end(); it++) {
        if ( *it == obs) {
            obs_.erase(it);
            break;
        }
    }
}
