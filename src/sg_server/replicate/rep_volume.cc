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
#include "sg_server/gc_task.h"
#include "sg_server/sg_util.h"
#include "sg_server/transfer/net_sender.h"
using huawei::proto::VolumeMeta;
using huawei::proto::REPLICATOR;
using huawei::proto::REP_PRIMARY;
const uint64_t DEADLINE = 300; // TODO:

RepVolume::RepVolume(const string& vol_id,
        std::shared_ptr<VolumeMetaManager> vol_mgr):
        vol_id_(vol_id),
        vol_mgr_(vol_mgr),
        transient_state(false){
    init();
}
RepVolume::~RepVolume(){
}

void RepVolume::init(){
    load_volume_meta();
    last_rep_status_ = vol_meta_.info().rep_status();
    // TODO: recover replicate state processing
}

int RepVolume::get_priority()const{
    return priority_;
}
void RepVolume::set_priority(int p){
    priority_ = p;
    return ;
}

bool RepVolume::get_task_generating_flag()const{
    return task_generating_flag_.load();
}
void RepVolume::set_task_generating_flag(bool flag){
    task_generating_flag_.store(flag);
}

uint64_t RepVolume::get_last_served_time()const{
    return last_served_time_;
}

// TODO: if task failed, should consider the retry time gap, or loop too offen
std::shared_ptr<TransferTask> RepVolume::get_next_task(){
    last_served_time_ = time(nullptr);

    if(base_sync_state_ == NEED_SYNC){
        //replicator consuming to checkpoint/snap marker?
        int result = replicator_consumed_to_checkpoint();
        SG_ASSERT(result <= 0);
        if(result == 0){
        // whether base snapshot is created
            auto record_it = vol_meta_.records().rbegin();
            const string& cur_snap = record_it->snap_id();
            SnapStatus snap_status;
            StatusCode ret = SnapClientWrapper::instance().get_client()->QuerySnapshot(
                        vol_id_,cur_snap,snap_status);
            SG_ASSERT(ret == StatusCode::sOk);
            if(snap_status == huawei::proto::SNAP_CREATED){
                string pre_snap;
                if(0 == get_last_shared_snap(pre_snap)){
                    base_sync_state_ = SYNCING;
                    LOG_INFO << "start to sync diff snapshot: pre:" << pre_snap
                        << ",cur:" << cur_snap;
                    return generate_base_sync_task(pre_snap,cur_snap);
                }
            }
        }
    }

    SG_ASSERT(nullptr != replicator_);
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
    if(replicator_->has_journals_to_transfer())
        return true;
    // a base snapshot need sync
    if(base_sync_state_ == NEED_SYNC
        /* snapshot is created? */){
        return true;
    }
    if(!transient_state)
        return false;

    // check replicate status, stable the transient state if catch to
    // checkpoint journal marker
    if(last_rep_status_ == REP_DISABLING
        || last_rep_status_ == REP_FAILING_OVER){
        int result = replicator_consumed_to_checkpoint();
        if(result >= 0){
            // update replication status if replicator consumed to checkpoint
            // TODO: wait for snapshot created??
            RepStatus s = last_rep_status_==REP_DISABLING ? REP_DISABLED:REP_FAILED_OVER;
            vol_meta_.mutable_info()->set_rep_status(s);
            persist_replication_status();
            LOG_INFO << vol_id_ << " update replicate status to " << s;
        }
    }

    return false;
}

void RepVolume::delete_rep_volume(){
    LOG_INFO << "replicate:remove volume[" << vol_id_ << "]";
    GCTask::instance().unregister_consumer(vol_id_,REPLICATOR);
    clean_up();
}

bool RepVolume::operator <(const RepVolume& other){
    uint64_t now = time(NULL);
    if(now - this->get_last_served_time() > DEADLINE 
        || now - other.get_last_served_time() > DEADLINE)
        return this->get_last_served_time() >  other.get_last_served_time(); // earlier has higher priority
    if(this->get_priority() != other.get_priority())
        return this->get_priority() < other.get_priority(); // bigger priority id has higher priority
    return this->get_last_served_time() < other.get_last_served_time();
}

void RepVolume::notify_rep_state_changed(){
    load_volume_meta();
    if(last_rep_status_ != vol_meta_.info().rep_status()){
        LOG_INFO << "volume[" << vol_id_ << "] replicate status changed:"
            << last_rep_status_ << " --> " << vol_meta_.info().rep_status();
        last_rep_status_ = vol_meta_.info().rep_status();
        // set transient_state,which will be turned to stable state in main check loop
        if(vol_meta_.info().rep_status() == REP_ENABLING
            || vol_meta_.info().rep_status() == REP_DISABLING
            || vol_meta_.info().rep_status() == REP_FAILING_OVER
            || vol_meta_.info().rep_status() == REP_REVERSING){
            transient_state = true;
        }
        // set base sync state
        if(vol_meta_.info().rep_status() == REP_ENABLING
            || vol_meta_.info().rep_status() == REP_REVERSING){
            base_sync_state_ = NEED_SYNC;
        }
        // check whether need to register/unregister replicator
        if(vol_meta_.info().rep_status() == REP_ENABLING){
            GCTask::instance().register_consumer(vol_id_,replicator_.get());
        }
        else if(vol_meta_.info().rep_status() == REP_DISABLING
            || vol_meta_.info().rep_status() == REP_DELETING){
            GCTask::instance().unregister_consumer(vol_id_,REPLICATOR);
        }
    }
}

int RepVolume::load_volume_meta(){
    VolumeMeta meta;
    RESULT res = vol_mgr_->read_volume_meta(vol_id_,meta);
    SG_ASSERT(DRS_OK == res);
    vol_meta_.CopyFrom(meta);
    return 0;
}

int RepVolume::persist_replication_status(){
    RESULT res = vol_mgr_->update_volume_meta(vol_meta_);
    SG_ASSERT(DRS_OK != res);
    return 0;
}

std::shared_ptr<TransferTask> RepVolume::generate_base_sync_task(
                    const string& pre_snap,const string& cur_snap){
    // TODO:set journal key, remote journal counter should less than the snap entry j_counter
    auto record_it = vol_meta_.records().rbegin();
    int64_t counter;
    sg_util::extract_counter_from_object_key(record_it->marker().cur_journal(),counter);
    auto f = std::bind(&RepVolume::recycle_base_sync_task,this,std::placeholders::_1);
    std::shared_ptr<RepContext> ctx(new RepContext(vol_id_, counter,
                            MAX_JOURNAL_SIZE_FOR_SNAP_SYNC,false,std::ref(f)));
    std::shared_ptr<TransferTask> task(new DiffSnapTask(pre_snap,cur_snap,ctx));
    task->set_id(0);
    task->set_status(T_WAITING);
    task->set_ts(time(nullptr));
    return task;
}
void RepVolume::recycle_base_sync_task(std::shared_ptr<TransferTask> t){
    // TODO:
    SG_ASSERT(T_DONE == t->get_status());
    // set remote producer marker
    // persist replication base sync done
    // delete conresponding snapshots
    // update replication status
}

void RepVolume::clean_up(){
    // TODO: recycle resources
}

int RepVolume::get_last_shared_snap(string& snap_id){
    auto records = vol_meta_.records();
    auto record_it = records.rbegin();
    // TODO:  for enable only, todo failback/reprotect
    SG_ASSERT(REPLICATION_ENABLE == record_it->type());
    record_it++;
    SG_ASSERT(REPLICATION_DISABLE == record_it->type());
    SnapStatus snap_status;
    StatusCode ret = SnapClientWrapper::instance().get_client()->QuerySnapshot(
                vol_id_,record_it->snap_id(),snap_status);
    SG_ASSERT(ret == StatusCode::sOk);
    if(snap_status == huawei::proto::SNAP_CREATED){
        snap_id = record_it->snap_id();
        return 0;
    }
    else{
        return -1;
    }
}

int RepVolume::replicator_consumed_to_checkpoint(){
    auto records = vol_meta_.records();
    auto record_it = records.rbegin();
    const JournalMarker cp_m = record_it->marker();
    JournalMarker consumer_m;
    int result = replicator_->get_producer_marker(consumer_m);
    SG_ASSERT(0 == result);
    result = sg_util::marker_compare(consumer_m,cp_m);
    return result;
}
