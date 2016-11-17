#ifndef _BLOCK_QUEUE_H
#define _BLOCK_QUEUE_H
#include <unistd.h>
#include <limits.h>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
using namespace std;

template <typename T>
size_t default_get_entry_size(const T& t){
    return 0;
}

template <typename T>
class BlockingQueue
{
    /*function to get queue entry memory size */
    typedef size_t (*get_entry_size_fn) (const T& t);

public:
    BlockingQueue(){
        m_queue_max_items = UINT_MAX;
        m_queue_cur_mem   = 0;
        m_queue_max_mem   = UINT_MAX;
        m_getsize_fn      = default_get_entry_size;
        start();
    }

    BlockingQueue(size_t queue_max_items){
        m_queue_max_items = queue_max_items;
        m_queue_cur_mem   = 0;
        m_queue_max_mem   = UINT_MAX;
        m_getsize_fn      = default_get_entry_size;
        start();
    }

    BlockingQueue(size_t max_queue_mem, get_entry_size_fn entry_size_fn){
        m_queue_max_items = UINT_MAX;
        m_queue_cur_mem   = 0;
        m_queue_max_mem   = max_queue_mem;
        m_getsize_fn      = entry_size_fn;
        start();
    }

    BlockingQueue(size_t max_queue_item, 
                  size_t max_queue_mem, 
                  get_entry_size_fn entry_size_fn){
        m_queue_max_items = max_queue_item;
        m_queue_cur_mem   = 0;
        m_queue_max_mem   = max_queue_mem;
        m_getsize_fn      = entry_size_fn;
        start();
    }

    ~BlockingQueue(){
        stop();
    }

    void start(){
        std::unique_lock<std::mutex> mlock(m_mtx);
        m_run = true;
    }
    
    void stop(){
        {
            std::unique_lock<std::mutex> mlock(m_mtx);
            m_run = false; 
        }
        m_nofull_cond.notify_all();
        m_noempty_cond.notify_all();
    }

    bool push(const T& item){
        std::unique_lock<std::mutex> mlock(m_mtx); 
        size_t cur_item_size = m_getsize_fn(item); 

        while(true){
            if(m_run && (m_queue.size() < m_queue_max_items) && 
              ((m_queue_cur_mem + cur_item_size) < m_queue_max_mem)){
                break;
            }

            if(!m_run){
                return false; 
            }
            m_nofull_cond.wait(mlock);
        }

        m_queue.push_back(item);
        m_queue_cur_mem += cur_item_size;
        mlock.unlock();
        m_noempty_cond.notify_one();
        return true;
    }

    bool pop(T& item){
        std::unique_lock<std::mutex> mlock(m_mtx);
    
        while(true){
            /*queue not empty*/
            if(!m_queue.empty()){
                item = m_queue.front();
                m_queue.pop_front();
                size_t cur_item_size = m_getsize_fn(item);
                m_queue_cur_mem += cur_item_size;
                mlock.unlock();
                m_nofull_cond.notify_one();
                return true;
            } 
            
            /*thread exit*/
            if(!m_run){
                return false;
            }
            
            /*queue empty, wait to be notify*/
            m_noempty_cond.wait(mlock);
        }

        return false;       
   }
    
    T pop(){
        T e;
        bool ret = pop(e);
        return ret ? e : nullptr;
    }


    size_t entry_number()const{
        std::unique_lock<std::mutex> mlock(m_mtx);
        return m_queue.size(); 
    }
    
    size_t memory_size()const{
        std::unique_lock<std::mutex> mlock(m_mtx);
        return m_queue_cur_mem; 
    }

    bool empty()const{
        return entry_number() == 0 ? true : false; 
    }

    T& operator[](size_t idx){
        std::unique_lock<std::mutex> mlock(m_mtx);
        return m_queue[idx]; 
    }
    
private:
    bool                    m_run;

    std::deque<T>           m_queue;
    size_t                  m_queue_max_items; /*max entry in queue */

    size_t                  m_queue_cur_mem;   /*current entry memory size */
    size_t                  m_queue_max_mem;   /*max entry memory size*/
    get_entry_size_fn       m_getsize_fn;
    
    /*use mutable, otherwise compile error*/
    mutable std::mutex              m_mtx;     
    std::condition_variable m_nofull_cond; 
    std::condition_variable m_noempty_cond;
};

#endif
