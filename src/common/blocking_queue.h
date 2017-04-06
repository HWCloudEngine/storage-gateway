#ifndef _BLOCK_QUEUE_H
#define _BLOCK_QUEUE_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <unistd.h>
#include <limits.h>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
using namespace std;

template<typename DT> 
class BlockingQueue;

template<typename DT>
class DataQueue
{
public:
    friend class BlockingQueue<DT>;
    
    typedef typename deque<DT>::pointer data_pnt_t;
    typedef typename deque<bool>::pointer filled_pnt_t;
    
    struct position {
        position() : data_pnt(0), filled_pnt(0)	{}
        data_pnt_t data_pnt;
        filled_pnt_t filled_pnt;
    };

    bool pop(DT& data) {
        if(m_data_que.empty() || ! m_filled_que.front())
            return false;
        data = m_data_que.front();
        m_data_que.pop_front();
        m_filled_que.pop_front();
        return true;
    }
    
    void reserve(position& pos) {
        m_data_que.push_back(m_dummy);
        m_filled_que.push_back(false);
        pos.data_pnt = &m_data_que[m_data_que.size() -1];
        pos.filled_pnt = &m_filled_que[m_filled_que.size() -1];
    }
                
    void fill(const DT& data, const position& pos) {
        *pos.data_pnt = data;
        *pos.filled_pnt = true;
    }

    void push(const DT& data) {
        m_data_que.push_back(data);
        m_filled_que.push_back(true);
    }
    
    size_t size() const { 
        return m_data_que.size(); 
    } 

    bool empty() const { 
        return m_data_que.empty(); 
    } 

    DT& operator[](size_t idx){
        return m_data_que[idx]; 
    }
   
private :
    deque<DT> m_data_que;
    deque<bool> m_filled_que;
    DT m_dummy;
    
    DataQueue() {}
};

template<typename DT>
size_t default_get_entry_size(const DT& t){
    return 0;
}

template<typename DT>
class BlockingQueue
{
    /*function to get queue entry memory size */
    typedef size_t (*get_entry_size_fn) (const DT& t);

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

    typedef typename DataQueue<DT>::position position;

    // Push data to queue using reserve position
    bool push(const DT& item, const position& pos) {
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

        m_queue.fill(item, pos);
        m_queue_cur_mem += cur_item_size;
        mlock.unlock();
        m_noempty_cond.notify_one();
        return true;
    } 
    
    // Pop data from input queue and reserve position in the output queue
    bool pop(DT& data, BlockingQueue& outque, position& pos){
        if(!pop(data)){
            return false;        
        }

        outque.reserve_pos(pos);
        return true;
    }

    // Simple push
    bool push(const DT& item){
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

        m_queue.push(item);
        m_queue_cur_mem += cur_item_size;
        mlock.unlock();
        m_noempty_cond.notify_one();
        return true;
    }

    // Simple pop
    bool pop(DT& item){
        std::unique_lock<std::mutex> mlock(m_mtx);

        while(true){
            /*queue not empty*/
            if(m_queue.pop(item)){
                 size_t cur_item_size = m_getsize_fn(item);
                m_queue_cur_mem -= cur_item_size;
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

    bool empty() {
        unique_lock<std::mutex> lk(m_mtx);
        return m_queue.empty();
    }

    size_t size() {
        unique_lock<std::mutex> lk(m_mtx);
        return m_queue.size();
    }
    
    DT pop(){
        DT e;
        bool ret = pop(e);
        return ret ? e : nullptr;
    }

    size_t entry_number()const{
        unique_lock<std::mutex> lk(m_mtx);
        return m_queue.size(); 
    }
    
    size_t memory_size()const{
        std::unique_lock<std::mutex> mlock(m_mtx);
        return m_queue_cur_mem; 
    }

    bool empty()const{
        return entry_number() == 0 ? true : false; 
    }

    bool full()const {
        return entry_number() == m_queue_max_items ? true : false; 
    }

    DT& operator[](size_t idx){
        std::unique_lock<std::mutex> mlock(m_mtx);
        return m_queue[idx]; 
    }

private:
    void reserve_pos(position& pos) {
        unique_lock<std::mutex> lk(m_mtx);
        m_queue.reserve(pos);
    } 

private:
    bool                    m_run;

    DataQueue<DT>           m_queue;
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
