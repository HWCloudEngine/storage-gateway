#ifndef PRQUEUE_H
#define PRQUEUE_H

/** PRQueue - queue with position reservation for multi-threaded programs
** 
** Copyright (C) 2009  Eugene Surman
** You may distribute under the terms of the GNU General Public License
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>

using namespace std; 

template< typename DT> 
class PRQueue;

template< typename DT>
class DataQueue
{
public:
    friend class PRQueue<DT>;
    
    typedef typename deque< DT>::pointer data_pnt_t;
    typedef typename deque< bool>::pointer filled_pnt_t;
    
    struct position {
        position() : data_pnt(0), filled_pnt(0)	{}
        data_pnt_t data_pnt;
        filled_pnt_t filled_pnt;
    };

    bool pop(DT& data) {
        if( m_data_que.empty() || ! m_filled_que.front())
            return false;
        data = m_data_que.front();
        m_data_que.pop_front();
        m_filled_que.pop_front();
        return true;
    }
    
    void reserve(position& pos) {
        /*reserve*/
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
    
private :
    deque<DT> m_data_que;
    deque<bool> m_filled_que;
    DT m_dummy;
    
    DataQueue() {}
};

template <typename DT >
class PRQueue
{
public:
    typedef typename DataQueue<DT>::position position;

    PRQueue() { 
    }

    // Push data to queue using reserve position
    void push(const DT& data, const position& pos) {
        unique_lock<std::mutex> lk(m_mux);
        m_que.fill(data, pos);
        m_not_empty.notify_all();
    } 
    
    // Pop data from input queue and reserve position in the output queue
    void pop(DT& data, PRQueue& outque, position& pos){
        {
            unique_lock<std::mutex> lk(m_mux);
            while(true) {
                if(m_que.pop(data))
                    break;
                m_not_empty.wait(lk);
            }
        }
        outque.reserve_pos(pos);
    }

    // Simple push
    void push(const DT& data) {
        unique_lock<std::mutex> lk(m_mux);
        m_que.push(data);
        m_not_empty.notify_all();
    } 

    // Simple pop
    void pop(DT& data) {
        unique_lock<std::mutex> lk(m_mux);
        while(true) {
            if(m_que.pop(data)) 
                break;
            m_not_empty.wait(lk);
        }
    }
    
    bool empty() {
        unique_lock<std::mutex> lk(m_mux);
        return m_que.empty();
    }

    size_t size() {
        unique_lock<std::mutex> lk(m_mux);
        return m_que.size();
    }
    
protected:
    DataQueue<DT> m_que;
    
    mutex  m_mux;
    condition_variable m_not_empty;

    void reserve_pos(position& pos) {
        unique_lock<std::mutex> lk(m_mux);
        m_que.reserve(pos);
    } 

};


#endif //PRQUEUE_H
