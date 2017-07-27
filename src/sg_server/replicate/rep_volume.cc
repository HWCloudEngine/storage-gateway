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
#include "task_handler.h"
#include "snapshot/snapshot_mgr.h"
using huawei::proto::VolumeMeta;
using huawei::proto::REPLICATOR;
using huawei::proto::REP_PRIMARY;
const uint64_t DEADLINE = 300; // TODO:

RepVolume::RepVolume(const string& vol_id,
        std::shared_ptr<VolumeMetaManager> vol_mgr,
        std::shared_ptr<JournalMetaManager> j_mgr):
        vol_id_(vol_id),
        vol_mgr_(vol_mgr),
        journal_mgr_(j_mgr),
        task_generating_flag_(false),
        transient_state(false),
        base_sync_state_(NO_SYNC){
    load_volume_meta();
}
RepVolume::~RepVolume(){
}

void RepVolume::recover_replication(){
    load_volume_meta();
    last_rep_status_ = vol_meta_.info().rep_status();
    // recover replicate state processing
    switch(last_rep_status_){
        case REP_ENABLING:
        {
            transient_state = true;
            auto last_record = vol_meta_.records().rbegin();
            if(last_record->is_synced() == false){
                base_sync_state_ = NEED_SYNC;
                LOG_INFO << "init repvolume[" << vol_id_ << "] rep status:"
                    << last_rep_status_;
            }
            else{
                resume_replicate();
            }
            break;
        }
        case REP_FAILING_OVER:
        case REP_DISABLING:
            transient_state = true;
            break;
        default:
            break;
    }
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

    if(base_sync_state_ == SYNCING && sync_task_->get_status() == T_ERROR){
        sync_task_->reset();
        sync_task_->set_status(T_WAITING);
        LOG_INFO << "redo base sync task.";
        return sync_task_;
    }

    if(base_sync_state_ == NEED_SYNC){
        // whether base snapshot is created
        auto record_it = vol_meta_.records().rbegin();
        const string& cur_snap = record_it->snap_id();
        SnapStatus snap_status;
        StatusCode ret = SnapshotMgr::singleton().Query(
                    vol_id_,cur_snap,snap_status);
        if(ret != StatusCode::sOk){
            LOG_WARN << "query snapshot failed:" << ret << ",volume=" << vol_id_;
            return nullptr;
        }
        if(snap_status == huawei::proto::SNAP_CREATED){
            string pre_snap;
            if(0 == get_last_shared_snap(pre_snap)){
                LOG_INFO << "start to sync diff snapshot: pre:" << pre_snap
                    << ",cur:" << cur_snap;
                sync_task_ = generate_base_sync_task(pre_snap,cur_snap,true);
            }
            else{
                // TODO:remote volume should be clean, or fullfilled with zero
                LOG_WARN << "pre snapshot of " << cur_snap << " not created!";
                LOG_INFO << "start to sync base snapshot:" << cur_snap;
                sync_task_ = generate_base_sync_task(pre_snap,cur_snap,false);
            }
            base_sync_state_ = SYNCING;
            return sync_task_;
        }
        LOG_DEBUG << "snapshot " << cur_snap << " not created.";
        // should not replicate any journals if sync is not done
        return nullptr;
    }

    if(base_sync_state_ == NO_SYNC){
        SG_ASSERT(nullptr != replicator_);
        return replicator_->get_next_replicate_task();
    }
    return nullptr;
}

void RepVolume::register_replicator(
        std::shared_ptr<ReplicatorContext> reptr) {
    replicator_ = reptr;
    if(vol_meta_.info().role() == REP_PRIMARY
        && vol_meta_.info().rep_status() != REP_DISABLED
        && vol_meta_.info().rep_status() != REP_FAILED_OVER){
        GCTask::instance().register_consumer(vol_id_,reptr.get());
    }
    replicator_->set_peer_volume(get_peer_volume());
}

bool RepVolume::need_replicate(){
    if(last_rep_status_ == REP_DISABLED
        || last_rep_status_ == REP_FAILED_OVER)
        return false;

    if(base_sync_state_ != SYNCING && replicator_->has_journals_to_transfer())
        return true;
    // a base snapshot need sync
    if(base_sync_state_ == NEED_SYNC
        || (base_sync_state_ == SYNCING && sync_task_->get_status() == T_ERROR)){
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
            std::lock_guard<std::mutex> lck(vol_meta_mtx);
            vol_meta_.mutable_info()->set_rep_status(s);
            persist_replication_status();
            LOG_INFO << vol_id_ << " update replicate status to " << s;
            last_rep_status_ = s;
            transient_state = false;
            // unregister consumer when disabled/failedover
            GCTask::instance().unregister_consumer(vol_id_,REPLICATOR);
        }
        else{
            return true;
        }
    }

    return false;
}

void RepVolume::delete_rep_volume(){
    LOG_INFO << "replicate:remove volume[" << vol_id_ << "]";
    GCTask::instance().unregister_consumer(vol_id_,REPLICATOR);

    // cancel all tasks
    replicator_->cancel_all_tasks();
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
    std::unique_lock<std::mutex> lck(vol_meta_mtx);
    load_volume_meta();
    lck.unlock();
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
        if(vol_meta_.info().rep_status() == REP_ENABLING){
            base_sync_state_ = NEED_SYNC;
        }
        // check whether need to register/unregister replicator
        if(vol_meta_.info().rep_status() == REP_ENABLING){
            GCTask::instance().register_consumer(vol_id_,replicator_.get());
        }
        else if(vol_meta_.info().rep_status() == REP_DELETING){
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
    SG_ASSERT(DRS_OK == res);
    return 0;
}

std::shared_ptr<TransferTask> RepVolume::generate_base_sync_task(
            const string& pre_snap,const string& cur_snap,bool has_pre_snap){
    // TODO:set journal key, remote journal counter should less than the snap entry j_counter
    auto record_it = vol_meta_.records().rbegin();
    uint64_t counter;
    SG_ASSERT(true == sg_util::extract_major_counter_from_journal_key(
            record_it->marker().cur_journal(),counter));

    auto f = std::bind(&RepVolume::recycle_base_sync_task,this,std::placeholders::_1);
    // NOTE: use peer volume id
    std::shared_ptr<RepContext> ctx(new RepContext(vol_id_,get_peer_volume(), counter,
                            g_option.journal_max_size,false,std::ref(f)));
    std::shared_ptr<TransferTask> task;
    if(has_pre_snap){
        task.reset(new DiffSnapTask(pre_snap,cur_snap,ctx));
    }
    else{
        task.reset(new BaseSnapTask(cur_snap,vol_meta_.info().size(),ctx));
    }
    task->set_id(0);
    task->set_status(T_WAITING);
    task->set_ts(time(nullptr));

    LOG_INFO << "generate base sync task:" << task->get_id()
        << " volume:" << vol_id_
        << " pre_snap:" << pre_snap
        << " cur_snap:" << cur_snap
        << " journal counter start at:" << std::hex << counter << std::dec;
    return task;
}

void RepVolume::recycle_base_sync_task(std::shared_ptr<TransferTask> t){
    SG_ASSERT(T_DONE == t->get_status());

    std::unique_lock<std::mutex> lck(vol_meta_mtx);
    load_volume_meta();
    auto last_record = vol_meta_.mutable_records()->rbegin();
    LOG_INFO << "last replicate record:"
        << ", volume:" << vol_meta_.info().vol_id()
        << ", volume status:" << vol_meta_.info().vol_status()
        << ", rep status:" << vol_meta_.info().rep_status()
        << ", operate id:" << last_record->operate_id()
        << "  type:" << last_record->type()
        << "  snap id:" << last_record->snap_id()
        << "  marker:" << last_record->marker().cur_journal()
        << ":" << last_record->marker().pos()
        << "  synced:" << last_record->is_synced();

    // persist replication base sync done
    last_record->set_is_synced(true);
    persist_replication_status();
    lck.unlock();

    resume_replicate();
}

void RepVolume::resume_replicate(){
    std::unique_lock<std::mutex> lck(vol_meta_mtx);
    auto last_record = vol_meta_.mutable_records()->rbegin();

    // delete conresponding snapshots
    SnapshotCtrlClient* snap_cli = create_snapshot_rpc_client(vol_id_);
    if(snap_cli != nullptr){
        string pre_snap;
        if(0 == get_last_shared_snap(pre_snap)){
            StatusCode status = snap_cli->DeleteSnapshot(vol_id_,pre_snap);
            if(status){
                LOG_ERROR << "delete snapshot[" << pre_snap << "] failed!";
            }
        }
        StatusCode status = snap_cli->DeleteSnapshot(vol_id_,last_record->snap_id());
        if(status){
            LOG_ERROR << "delete snapshot[" << last_record->snap_id() << "] failed!";
        }
        destroy_snapshot_rpc_client(snap_cli);
    }
    else {
        LOG_ERROR << "get snapshot rpc client failed!";
    }

    // set remote producer marker& replicator consumer marker
    uint64_t counter;
    SG_ASSERT(true == sg_util::extract_major_counter_from_journal_key(
            last_record->marker().cur_journal(),counter));
    counter++; // move to next journal
    JournalMarker marker;
    marker.set_cur_journal(sg_util::construct_journal_key(vol_id_,counter,0));
    marker.set_pos(0);
    TaskHandler::instance().add_marker_context(replicator_.get(),marker);

    // update replicator consumer directly, since remote producer marker was
    // updated async, if crushed right after replicate stated changed to
    // REP_ENABLED, replicator consumer marker now is invalid
    replicator_->update_consumer_marker(marker);
    LOG_INFO << "update consumer marker:" << marker.cur_journal()
        << ":" << marker.pos();

    // update replication status,update status to enable will unhold producer marker
    vol_meta_.mutable_info()->set_rep_status(REP_ENABLED);
    persist_replication_status();
    last_rep_status_ = REP_ENABLED;
    LOG_INFO << "update volume[" << vol_id_ << "] replicate status to enabled ";

    // re-init replicator context
    if(0 != replicator_->init()){
        LOG_ERROR << "re-init replicatorContext failed!";
    }
    base_sync_state_ = NO_SYNC;
    sync_task_.reset();
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
    if(REPLICATION_DISABLE != record_it->type()){ // look for pre snapshot
        SG_ASSERT(REPLICATION_REVERSE == record_it->type());
        record_it++;
        if(REPLICATION_FAILOVER != record_it->type()){
            LOG_WARN << "pre replicate operation is neither failover nor disable\
                ,volume:" << vol_id_;
            return -1;
        }
    }

    SnapStatus snap_status;
    StatusCode ret = SnapshotMgr::singleton().Query(
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
    int result = replicator_->get_consumer_marker(consumer_m);
    SG_ASSERT(0 == result);
    result = journal_mgr_->compare_marker(consumer_m,cp_m);
    return result;
}

string RepVolume::get_peer_volume(){
    SG_ASSERT(vol_meta_.info().peer_volumes_size() == 1);
    return vol_meta_.info().peer_volumes(0);
}
