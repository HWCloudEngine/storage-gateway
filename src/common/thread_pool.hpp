/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
* File name:    thread_pool.hpp
* Author:
* Date:         2016/11/09
* Version:      1.0
* Description:
*
***********************************************/
#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <queue>
#include <mutex>
#include <cstddef>
#include "blocking_queue.h"
namespace sg_threads{
class JoinThreads
{
    std::vector<std::thread>& threads;
public:
    explicit JoinThreads(std::vector<std::thread>& threads_):
        threads(threads_)
    {}
    ~JoinThreads()
    {
        for(unsigned long i=0; i<threads.size(); ++i)
        {
            if(threads[i].joinable())
                threads[i].join();
        }
    }
};

class ThreadPool
{
    std::atomic_bool done;
    std::unique_ptr<BlockingQueue<std::function<void ()>>> work_queue;
    std::vector<std::thread> threads;
    JoinThreads joiner;
    void worker_thread()
    {
        while(!done)
        {
            std::function<void ()> task;
            if(work_queue->pop(task))
            {
                task();
            }
            else
            {
                std::this_thread::yield();
            }
        }
    }
public:
    ThreadPool(const int& thread_count):
        done(false),
        joiner(threads),
        work_queue(new BlockingQueue<std::function<void ()>>(thread_count*2))
    {
        try
        {
            for(unsigned i=0; i<thread_count; ++i)
            {
                threads.push_back(std::thread(&ThreadPool::worker_thread,this));
            }
        }
        catch(...)
        {
            done=true;
            work_queue->stop();
            throw;
        }
    }
    ThreadPool():ThreadPool(std::thread::hardware_concurrency()>0?
        std::thread::hardware_concurrency():2)
    {
    }
    ~ThreadPool()
    {
        done=true;
        work_queue->stop();
    }

    template<typename FunctionType>
    bool submit(FunctionType f)
    {
        return work_queue->push(std::function<void()>(f));
    }
};
};

#endif
