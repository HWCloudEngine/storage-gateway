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
#include <mutex>
#include "replicator_context.h"
#include "../volume_meta_manager.h"
#include "sg_server/journal_meta_manager.h"
#include "rep_task.h"
typedef enum SyncStatus{
    NO_SYNC = 0,
    NEED_SYNC,
    SYNCING,
}SyncStatus;

class RepVolume{
    std::string vol_id_;
    int priority_;
    Configure& conf_;
    // last served time
    uint64_t last_served_time_;
    std::shared_ptr<ReplicatorContext> replicator_;
    SyncStatus base_sync_state_;
    // sync task
    std::shared_ptr<TransferTask> sync_task_;
    std::mutex vol_meta_mtx;
    VolumeMeta vol_meta_;
    RepStatus last_rep_status_;
    bool transient_state;
    std::atomic<bool> task_generating_flag_;
    std::shared_ptr<VolumeMetaManager> vol_mgr_;
    std::shared_ptr<JournalMetaManager> journal_mgr_;
public:
    RepVolume(const string& vol_id,Configure& conf,
            std::shared_ptr<VolumeMetaManager> vol_mgr,
            std::shared_ptr<JournalMetaManager> j_mgr);
    ~RepVolume();
    void register_replicator(std::shared_ptr<ReplicatorContext> reptr);
    int get_priority()const;
    void set_priority(int p);
    uint64_t get_last_served_time()const;
    // get next replicate task
    std::shared_ptr<TransferTask> get_next_task();
    void notify_rep_state_changed();
    bool need_replicate();
    void delete_rep_volume();
    bool operator <(const RepVolume& other);
    bool get_task_generating_flag()const;
    void set_task_generating_flag(bool flag);
    // check transient replication state, and try recover
    void recover_replication();
private:
    int load_volume_meta();
    int persist_replication_status();
    // generate a task to sync base snapshot
    std::shared_ptr<TransferTask> generate_base_sync_task(
        const string& pre_snap,const string& cur_snap,bool has_pre_snap);
    void recycle_base_sync_task(std::shared_ptr<TransferTask> t);
    void resume_replicate();
    // recycle resources
    void clean_up();
    int get_last_shared_snap(string& snap_id);
    int replicator_consumed_to_checkpoint();
};
#endif
