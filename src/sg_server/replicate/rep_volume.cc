/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_volume.cc
* Author: 
* Date:         2016/11/15
* Version:      1.0
* Description:
* 
************************************************/
#include <time.h> // time,time_t
#include "rep_volume.h"
#include "log/log.h"
#include "../gc_task.h"
using huawei::proto::VolumeMeta;
using huawei::proto::REPLICATOR;
using huawei::proto::REP_PRIMARY;
const uint64_t DEADLINE = 300; // TODO:

RepVolume::RepVolume(const string& vol_id,
        std::shared_ptr<VolumeMetaManager> vol_mgr):
        vol_id_(vol_id),
        vol_mgr_(vol_mgr),
        task_generating_flag_(false){
    load_volume_meta();
}
RepVolume::~RepVolume(){
}

int RepVolume::get_priority()const{
    return priority_;
}
void RepVolume::set_priority(int p){
    priority_ = p;
    return ;
}
uint64_t RepVolume::get_last_served_time()const{
    return last_served_time_;
}
std::shared_ptr<RepTask> RepVolume::get_next_task(){
    last_served_time_ = time(nullptr);
    if(status_changed_ || get_base_sync_state()){
    // TODO: check replication status whether need base sync

    }

    if(!replicator_)
        return nullptr;
    return replicator_->get_next_replicate_task();
}

void RepVolume::register_replicator(
        std::shared_ptr<ReplicatorContext> reptr) {
    replicator_ = reptr;
    if(vol_meta_.info().role() == REP_PRIMARY
        && vol_meta_.info().rep_status() == REP_ENABLED){
        GCTask::instance().register_consumer(vol_id_,reptr.get());
    }
}

bool RepVolume::need_replicate(){
    //TODO: base data sync?

    return replicator_->has_journals_to_transfer();
}

bool RepVolume::get_task_generating_flag()const{
    return task_generating_flag_.load();
}
void RepVolume::set_task_generating_flag(bool flag){
    task_generating_flag_.store(flag);
}

void RepVolume::set_replication_status_changed(){
    status_changed_.store(true);
    load_volume_meta();
}

void RepVolume::delete_rep_volume(){
    GCTask::instance().unregister_consumer(vol_id_,REPLICATOR);
    clean_up();
}

bool RepVolume::operator <(const RepVolume& other){
    uint64_t now = time(NULL);
    if(now - this->get_last_served_time() > DEADLINE || now - other.get_last_served_time() > DEADLINE)
        return this->get_last_served_time() >  other.get_last_served_time(); // earlier has higher priority
    if(this->get_priority() != other.get_priority())
        return this->get_priority() < other.get_priority(); // bigger priority id has higher priority
    return this->get_last_served_time() < other.get_last_served_time();
}

int RepVolume::load_volume_meta(){
    VolumeMeta meta;
    RESULT res = vol_mgr_->read_volume_meta(vol_id_,meta);
    DR_ASSERT(DRS_OK == res);
    old_rep_status_ = meta.info().rep_status();
    vol_meta_.CopyFrom(meta);
    return 0;
    
}
int RepVolume::persist_replication_status(){
    RESULT res = vol_mgr_->update_volume_meta(vol_meta_);
    DR_ASSERT(DRS_OK != res);
    return 0;
}

void RepVolume::set_base_sync_state(int s){
    base_sync_state_ = s;
    return ;
}
int RepVolume::get_base_sync_state(){
    return base_sync_state_;
}

std::shared_ptr<RepTask> generate_base_sync_task(){
    // TODO:
    return 0;
}
void recycle_base_sync_task(std::shared_ptr<RepTask> t){
    // TODO:
}

void RepVolume::clean_up(){
    // TODO: recycle resources
}

