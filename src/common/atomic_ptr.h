/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   atomic_ptr.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: atomic pointer
*
*************************************************/
#ifndef SRC_COMMON_ATOMIC_PTR_H_
#define SRC_COMMON_ATOMIC_PTR_H_
#include <atomic>

class AtomicPtr {
 public:
     explicit AtomicPtr(void* v);

     void* lock_load();
     void  lock_store(void* v);
     void* nolock_load();
     void  nolock_store(void* v);

 private:
     std::atomic<void*> rep_;
};

#endif  // SRC_COMMON_ATOMIC_PTR_H_
