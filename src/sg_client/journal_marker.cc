/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_marker.cc 
*  Author: 
*  Date:         2017/6/22
*  Version:      1.0
*  Description:  handle marker 
*
*************************************************/
#include "journal_marker.h"
#include "log/log.h"


MarkerHandler::MarkerHandler(VolumeAttr& vol_attr):
vol_attr_(vol_attr),written_size_since_last_update(0LLU), producer_marker_hold_flag(false){
    epoll_fd = -1;
}

MarkerHandler::~MarkerHandler(){
    producer_event.unregister_from_epoll_fd(epoll_fd);
}

bool MarkerHandler::init(int _epoll_fd, std::shared_ptr<WriterClient> rpc_client,
    std::shared_ptr<CephS3LeaseClient> lease_client){
    rpc_client_ = rpc_client;
    lease_client_ = lease_client;
    int e_fd = eventfd(0, EFD_NONBLOCK|EFD_CLOEXEC);
    SG_ASSERT(e_fd != -1);
    producer_event.set_event_fd(e_fd);
    // Edge Triggered & wait for write event
    producer_event.set_epoll_events_type(EPOLLIN|EPOLLET);
    producer_event.set_epoll_data_ptr(reinterpret_cast<void*>(this));
    // register event
    epoll_fd = _epoll_fd;
    SG_ASSERT(0 == producer_event.register_to_epoll_fd(epoll_fd));
}

void MarkerHandler::clear_producer_event() {
    producer_event.clear_event();
}

void MarkerHandler::hold_producer_marker() {
    LOG_DEBUG << "hold producer marker,vol=" << vol_attr_.vol_name();
    producer_marker_hold_flag.store(true);
}

void MarkerHandler::unhold_producer_marker() {
    LOG_DEBUG << "unhold producer marker,vol=" << vol_attr_.vol_name();
    producer_marker_hold_flag.store(false);
}

bool MarkerHandler::is_producer_marker_holding() {
    return producer_marker_hold_flag.load();
}

JournalMarker MarkerHandler::get_cur_producer_marker() {
    std::lock_guard<std::mutex> lck(producer_mtx);
    return cur_producer_marker;
}

int MarkerHandler::update_producer_marker(const JournalMarker& marker) {
    if (false == rpc_client_->update_producer_marker(
            lease_client_->get_lease(), vol_attr_.vol_name(), marker)) {
        LOG_ERROR << "update volume[" << vol_attr_.vol_name() << "] producer marker failed!";
        return -1;
    }
    return 0;
}

void MarkerHandler::update_cached_marker(string journal,uint64_t pos){
    std::lock_guard<std::mutex> lck(producer_mtx);
    cur_producer_marker.set_cur_journal(journal);
    cur_producer_marker.set_pos(pos);
}

void MarkerHandler::update_written_size(uint64_t write_size){
    std::lock_guard<std::mutex> lck(producer_mtx);
    written_size_since_last_update += write_size;
}

void MarkerHandler::producer_update_trigger(){
    // to update producer marker if enough io were written
    if (producer_marker_hold_flag.load() == false
        && written_size_since_last_update >= g_option.journal_producer_written_size_threshold) {
        producer_event.trigger_event();
        written_size_since_last_update = 0;
        LOG_DEBUG << "trigger to update producer marker:" << vol_attr_.vol_name();
    }
}

VolumeAttr& MarkerHandler::get_vol_attr() {
    return vol_attr_;
}

