/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    journal_marker.h
* Author: 
* Date:         2017/06/22
* Version:      1.0
* Description:
* 
************************************************/
#ifndef JOURNAL_MARKER_Handler__
#define JOURNAL_MARKER_Handler__
#include <memory>
#include <atomic>
#include <string>

#include "epoll_event.h"
#include "common/ceph_s3_lease.h"
#include "common/volume_attr.h"
#include "common/config_option.h"
#include "rpc/clients/writer_client.h"


class MarkerHandler{
public:
    explicit MarkerHandler(VolumeAttr& vol_attr);
    virtual ~MarkerHandler();
    MarkerHandler(const MarkerHandler& m) = delete;
    MarkerHandler& operator=(const MarkerHandler& m) = delete;
    void clear_producer_event();
    void hold_producer_marker();
    void unhold_producer_marker();
    bool is_producer_marker_holding();
    JournalMarker get_cur_producer_marker();
    int update_producer_marker(const JournalMarker& marker);
    void update_cached_marker(std::string journal, uint64_t pos);
    void update_written_size(uint64_t write_size);
    void producer_update_trigger();
    bool init(int _epoll_fd, std::shared_ptr<WriterClient> rpc_client,
                std::shared_ptr<CephS3LeaseClient> lease_client);
    VolumeAttr& get_vol_attr();

private:
    // new written size in journals since last update of producer marker
    uint64_t written_size_since_last_update;
    EpollEvent producer_event;
    // whether to hold updating producer marker
    std::atomic<bool> producer_marker_hold_flag;
    // the producer marker which need update
    JournalMarker cur_producer_marker;
    // mutex for producer marker
    std::mutex producer_mtx;
    // epoll fd, created in VolumeMgr, which collects the writers' events
    int epoll_fd;
    VolumeAttr& vol_attr_;
    std::shared_ptr<WriterClient> rpc_client_;
    std::shared_ptr<CephS3LeaseClient> lease_client_;

};
#endif
