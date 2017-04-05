/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    epoll_event.h
* Author: 
* Date:         2017/03/02
* Version:      1.0
* Description:
* 
************************************************/
#ifndef EPOLL_EVENT_H_
#define EPOLL_EVENT_H_
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <mutex>
#include <cstring>
typedef struct epoll_event epoll_event_t;
class EpollEvent{
private:
    std::mutex mtx;
    epoll_event_t ev;
    int fd;
public:
    EpollEvent():fd(-1){
    }

    ~EpollEvent(){
        if(fd != -1)
            close(fd);
    }

    int register_to_epoll_fd(int epoll_fd){
        return epoll_ctl(epoll_fd,EPOLL_CTL_ADD,fd,&ev);
    }

    int unregister_from_epoll_fd(int epoll_fd){
        return epoll_ctl(epoll_fd,EPOLL_CTL_DEL,fd,&ev);
    }

    void set_event_fd(int _fd){
        fd = _fd;
    }

    const int& get_event_fd() const{
        return fd;
    }

    void set_epoll_event(const epoll_event_t& _ev) {
        std::memcpy(&ev,&_ev,sizeof(epoll_event_t));
    }
    
    const epoll_event_t& get_epoll_event() const{
        return ev;
    }

    void set_epoll_events_type(const uint32_t& _events){
        ev.events = _events;
    }

    void set_epoll_data_ptr(void* ptr) {
        ev.data.ptr = ptr;
    }

    void* get_epoll_data_ptr() const{
        return ev.data.ptr;
    }

    int get_epoll_data_fd()const {
        return ev.data.fd;
    }

    int trigger_event(){
        uint64_t buf = 1;
        std::lock_guard<std::mutex> lock(mtx);
        return write(fd,(char*)(&buf),sizeof(uint64_t));
    }

    int clear_event(){
        uint64_t buf;
        std::lock_guard<std::mutex> lock(mtx);
        return read(fd,(char*)(&buf),sizeof(uint64_t));
    }
};
#endif
