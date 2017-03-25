/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
* File name:    locks.h
* Author:
* Date:         2017/03/20
* Version:      1.0
* Description:
*
***********************************************/
#ifndef LOCKS_H_
#define LOCKS_H_
#include <boost/thread/shared_mutex.hpp>

/*boost read write lock */
typedef boost::shared_mutex       SharedMutex;
typedef boost::unique_lock<SharedMutex> WriteLock;
typedef boost::shared_lock<SharedMutex> ReadLock;
typedef boost::shared_lock<SharedMutex> ReadLock;

#endif
