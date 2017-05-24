/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   atomic_ptr.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: atomic pointer
*
*************************************************/

#include "atomic_ptr.h"

AtomicPtr::AtomicPtr(void* v) : rep_(v) {
}

void* AtomicPtr::lock_load() {
    return rep_.load(std::memory_order_acquire);
}

void AtomicPtr::lock_store(void* v) {
    rep_.store(v, std::memory_order_release);
}

void* AtomicPtr::nolock_load() {
    return rep_.load(std::memory_order_relaxed);
}

void AtomicPtr::nolock_store(void* v) {
    rep_.store(v, std::memory_order_relaxed);
}
