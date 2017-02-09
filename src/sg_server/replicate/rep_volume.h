/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_volume.h
* Author: 
* Date:         2016/11/15
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_VOLUME_H_
#define REP_VOLUME_H_
#include <atomic>
#include <string>
#include "replicator.h"
#include "rep_transmitter.h"
#include "../volume_meta_manager.h"
class RepVolume{
    std::string vol_id_;
    int priority_;
    // last served time
    uint64_t last_served_time_;
    std::shared_ptr<ReplicatorContext> replicator_;
    int base_sync_state_;
    VolumeMeta vol_meta_;
    REP_STATUS old_rep_status_;
    std::atomic<bool> status_changed_;
    std::atomic<bool> task_generating_flag_;
    std::shared_ptr<VolumeMetaManager> vol_mgr_;
public:
    RepVolume(const string& vol_id,
            std::shared_ptr<VolumeMetaManager> vol_mgr);
    ~RepVolume();
    void register_replicator(std::shared_ptr<ReplicatorContext> reptr);
    int get_priority();
    void set_priority(int p);
    uint64_t get_last_served_time();
    int serve();
    int sync_volume_meta();
    bool need_replicate();
    bool get_task_generating_flag();
    void set_task_generating_flag(bool flag);
    void set_replication_status_changed();
    void to_delete();
private: 
    int update_replication_status();
    void set_base_sync_state(int s);
    int get_base_sync_state();
    int submit_base_sync_task();
    void recycle_base_sync_task(std::shared_ptr<RepTask> t);
};
#endif
