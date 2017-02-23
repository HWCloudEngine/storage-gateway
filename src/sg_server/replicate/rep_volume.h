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
#include "replicator_context.h"
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
    RepStatus last_rep_status_;
    std::atomic<bool> status_changed_;
    std::atomic<bool> task_generating_flag_;
    std::shared_ptr<VolumeMetaManager> vol_mgr_;
public:
    RepVolume(const string& vol_id,
            std::shared_ptr<VolumeMetaManager> vol_mgr);
    ~RepVolume();
    void register_replicator(std::shared_ptr<ReplicatorContext> reptr);
    int get_priority()const;
    void set_priority(int p);
    uint64_t get_last_served_time()const;
    // get next replicate task
    std::shared_ptr<RepTask> get_next_task();
    int load_volume_meta();
    bool need_replicate();
    bool get_task_generating_flag()const;
    void set_task_generating_flag(bool flag);
    void set_replication_status_changed();
    void delete_rep_volume();
    bool operator <(const RepVolume& other);
private: 
    int persist_replication_status();
    void set_base_sync_state(int s);
    int get_base_sync_state();
    // generate a task to sync base snapshot
    std::shared_ptr<RepTask> generate_base_sync_task();
    void recycle_base_sync_task(std::shared_ptr<RepTask> t);
    // recycle resources
    void clean_up();
};
#endif
