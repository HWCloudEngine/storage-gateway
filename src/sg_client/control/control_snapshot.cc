/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:    control_snapshot.h
*  Author:
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  snapshot control interface export to highlevel control layer 
*
*************************************************/
#include <fstream>
#include "common/env_posix.h"
#include "log/log.h"
#include "control_snapshot.h"
using huawei::proto::StatusCode;

SnapshotControlImpl::SnapshotControlImpl(
        map<string, shared_ptr<Volume>>& volumes) : m_volumes(volumes) {
    m_pending_queue = new BlockingQueue<struct BgJob*>(10);
    m_complete_queue = new deque<struct BgJob*>();
    m_run = true;
    m_work_thread = new thread(&SnapshotControlImpl::bg_work, this);
    m_reclaim_thread = new thread(&SnapshotControlImpl::bg_reclaim, this);
}

SnapshotControlImpl::~SnapshotControlImpl() {
    m_pending_queue->stop();
    m_run = false;
    m_reclaim_thread->join();
    m_work_thread->join();
    delete m_reclaim_thread;
    delete m_work_thread;
    delete m_pending_queue;
    delete m_complete_queue;
    m_volumes.clear();
}

shared_ptr<SnapshotProxy> SnapshotControlImpl::get_vol_snap_proxy(const string& vol_name) {
    auto it = m_volumes.find(vol_name);
    if (it != m_volumes.end()) {
        return it->second->get_snapshot_proxy();
    }
    LOG_ERROR << "get volume snaphsot proxy volume" << vol_name << " failed";
    return nullptr;
}

Status SnapshotControlImpl::CreateSnapshot(ServerContext* context,
                                           const CreateSnapshotReq* req,
                                           CreateSnapshotAck* ack) {
    /*find volume*/
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "rpc create snapshot vloume:" << vname << " snap:" << sname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc create snapshot vloume:" << vname << " snap:" << sname
                 << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->create_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc create snapshot volume:" << vname << " snap:" << sname << " error:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc create snapshot vname:" << vname << " snap:" << sname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::ListSnapshot(ServerContext* context,
                                         const ListSnapshotReq* req,
                                         ListSnapshotAck* ack) {
    string vname = req->vol_name();
    LOG_INFO << "rpc list snapshot volume:" << vname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc list snapshot vloume:" << vname << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->list_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc list snapshot volume:" << vname << " failed" << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc list snapshot volume:" << vname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::QuerySnapshot(ServerContext* context,
                                          const QuerySnapshotReq* req,
                                          QuerySnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "rpc query snapshot volume:" << vname << " snap:" << sname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc qurey snapshot vloume:" << vname << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->query_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc query snapshot volume:" << vname << " snap:" << sname << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc query snapshot volume:" << vname << " snap:" << sname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::DeleteSnapshot(ServerContext* context,
                                           const DeleteSnapshotReq* req,
                                           DeleteSnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "rpc delete snapshot volume:" << vname << " snap:" << sname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc delete snapshot vloume:" << vname << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->delete_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc delete snapshot volume:" << vname << " snap:" << sname << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc delete snapshot volume:" << vname << " snap:" << sname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::RollbackSnapshot(ServerContext* context,
                                             const RollbackSnapshotReq* req,
                                             RollbackSnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "rpc rollback snapshot volume:" << vname << " snap:" << sname;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc rollback snapshot vloume:" << vname << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->rollback_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc rollback snapshot volume:" << vname << " snap:" << sname << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc rollback snapshot volume:" << vname << " snap:" << sname << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::DiffSnapshot(ServerContext* context,
                                          const DiffSnapshotReq* req,
                                          DiffSnapshotAck* ack) {
    string vname = req->vol_name();
    string first_snap = req->first_snap_name();
    string last_snap = req->first_snap_name();
    LOG_INFO << "rpc diff snapshot volume:" << vname
        << " first_snap:" << first_snap << " last_snap:" << last_snap;
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc diff snapshot vloume:" << vname << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->diff_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc diff snapshot volume:" << vname
            << " first_snap:" << first_snap << " last_snap:" << last_snap << " failed";
        return Status::OK;
    }
    LOG_INFO << "rpc diff snapshot volume:" << vname
        << " first_snap:" << first_snap << " last_snap:" << last_snap << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::ReadSnapshot(ServerContext* context,
                                         const ReadSnapshotReq* req,
                                         ReadSnapshotAck* ack) {
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "rpc read snapshot volume:" << vname << " snap:" << sname; 
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vname);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << "rpc read snapshot vloume:" << vname << " volume not exist";
        ack->mutable_header()->set_status(StatusCode::sVolumeNotExist);
        return Status::OK;
    }
    /*dispatch to volume*/
    StatusCode ret = vol_snap_proxy->read_snapshot(req, ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << "rpc read snapshot volume:" << vname << " snap:" << sname << " err:" << ret;
        return Status::OK;
    }
    LOG_INFO << "rpc read snapshot volume:" << vname << " snap:" << sname
             << "data size:" << ack->data().size()
             << "data_len:" << ack->data().length() << " ok";
    return Status::OK;
}

bool SnapshotControlImpl::is_bdev_available(const string& blk_device) {
    int ret = access(blk_device.c_str(), F_OK);
    if (ret) {
        LOG_ERROR << " block device:" << blk_device << "not available";
        return false;
    }
    return true;
}

bool SnapshotControlImpl::is_snapshot_available(const string& vol,
                                                const string& snap) {
    shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(vol);
    if (vol_snap_proxy == nullptr) {
        LOG_ERROR << " vol:" << vol << " not exist";
        return false;
    }
    QuerySnapshotReq req;
    QuerySnapshotAck ack;
    req.set_vol_name(vol);
    req.set_snap_name(snap);
    auto ret = vol_snap_proxy->query_snapshot(&req, &ack);
    if (ret != StatusCode::sOk) {
        LOG_ERROR << " vol:" << vol << " snap:" << snap << " status error";
        return false;
    }
    return ack.snap_status() == SnapStatus::SNAP_CREATED ? true : false;
}

Status SnapshotControlImpl::CreateVolumeFromSnap(ServerContext* context,
         const CreateVolumeFromSnapReq* req, CreateVolumeFromSnapAck* ack) {
    string new_volume = req->new_vol_name();
    string new_blk_device = req->new_blk_device();
    string vname = req->vol_name();
    string sname = req->snap_name();
    LOG_INFO << "rpc create volume from snapshot volume:" << vname << " snap:" << sname
        << " new_volume:" << new_volume << " new_device:" << new_blk_device;
    if (!is_bdev_available(new_blk_device)) {
        LOG_ERROR << "rpc create volume from snapshot vname:" << vname << " failed new blk not ready";
        ack->mutable_header()->set_status(StatusCode::sSnapCreateVolumeBusy);
        return Status::OK;
    }
    if (!is_snapshot_available(vname, sname)) {
        LOG_ERROR << "rpc create volume from snapshot vname:" << vname << " failed snapshot not ready";
        ack->mutable_header()->set_status(StatusCode::sSnapCreateVolumeBusy);
        return Status::OK;
    }
    if (m_pending_queue->full()) {
        LOG_ERROR << "rpc create volume from snapshot vname:" << vname << "queue full failed";
        ack->mutable_header()->set_status(StatusCode::sSnapCreateVolumeBusy);
        return Status::OK;
    }
    struct BgJob* job = new BgJob(new_volume, new_blk_device, vname, sname);
    job->status = BG_INIT;
    m_pending_queue->push(job);
    ack->mutable_header()->set_status(StatusCode::sOk);
    LOG_INFO << "rpc create volume from snapshot volume:" << vname << " snap:" << sname
        << " new_volume:" << new_volume << " new_device:" << new_blk_device << " ok";
    return Status::OK;
}

Status SnapshotControlImpl::QueryVolumeFromSnap(ServerContext* context,
            const QueryVolumeFromSnapReq* req, QueryVolumeFromSnapAck* ack) {
    string new_volume = req->new_vol_name();
    LOG_INFO << "rpc query volume from snapshot volume:" << new_volume;
    for (int i = 0; i < m_pending_queue->size(); i++) {
        struct BgJob* job = (*m_pending_queue)[i];
        if (job->new_volume.compare(new_volume) == 0) {
            ack->set_vol_status(VolumeStatus::VOL_ENABLING);
            LOG_INFO << "volume:" << new_volume << " still restoring";
            return Status::OK;
        }
    }
    for (int i = 0; i < m_complete_queue->size(); i++) {
        struct BgJob* job = (*m_complete_queue)[i];
        if (job->new_volume.compare(new_volume) == 0) {
            LOG_INFO << "volume:" << new_volume << " already restored";
            ack->set_vol_status(VolumeStatus::VOL_AVAILABLE);
            return Status::OK;
        }
    }
    LOG_INFO << "rpc query volume from snapshot volume:" << new_volume << " ok";
    return Status::OK;
}

void SnapshotControlImpl::bg_work() {
    while (m_run) {
        struct BgJob* job = m_pending_queue->pop();
        if (job == nullptr) {
            LOG_ERROR << " bgjob is nullptr";
            return;
        }
        job->status = BG_DOING;
        gettimeofday(&(job->start_ts), NULL);
        unique_ptr<AccessFile> block_file;
        Env::instance()->create_access_file(job->new_blk_device, false, true, &block_file);
        size_t bdev_size = Env::instance()->file_size(job->new_blk_device);
        off_t  bdev_off = 0;
        size_t bdev_slice = COW_BLOCK_SIZE;
        shared_ptr<SnapshotProxy> vol_snap_proxy = get_vol_snap_proxy(job->vol_name);
        if (vol_snap_proxy == nullptr) {
            LOG_ERROR << " bgjob volume:" << job->vol_name << " not exist";
            return;
        }
        ReadSnapshotReq req;
        ReadSnapshotAck ack;
        req.set_vol_name(job->vol_name);
        req.set_snap_name(job->snap_name);
        LOG_INFO << "bgjob restore vol:" << job->new_volume << " size:" << bdev_size;
        while (bdev_off < bdev_size) {
            bdev_slice = ((bdev_size-bdev_off) > COW_BLOCK_SIZE) ? COW_BLOCK_SIZE : (bdev_size-bdev_off);
            req.set_off(bdev_off);
            req.set_len(bdev_slice);
            StatusCode ret = vol_snap_proxy->read_snapshot(&req, &ack);
            if (ret != StatusCode::sOk) {
                LOG_ERROR << "bgjob read snapshot failed ret:" << ret;
                break;
            }
            assert(ret == StatusCode::sOk);
            ssize_t write_ret = block_file->write(const_cast<char*>(ack.data().c_str()), ack.data().length(), bdev_off);
            if (write_ret != ack.data().length()) {
                LOG_ERROR << "bgjob write block device failed write_ret:" << write_ret << " len:" << ack.data().length();
                break;
            }
            bdev_off += bdev_slice;
        }
        LOG_INFO << "bgjob restore vol:" << job->new_volume << " size:" << bdev_size << " ok";
        job->status = BG_DONE;
        gettimeofday(&(job->complete_ts), NULL);
        gettimeofday(&(job->expire_ts), NULL);
        job->expire_ts.tv_sec += (60*60*12);  // 60 hours will expire
        LOG_INFO << "bgjob new_vol:" << job->new_volume  << " work done";
        m_complete_queue->push_back(job);
    }
}

void SnapshotControlImpl::bg_reclaim() {
    while (m_run) {
        if (m_complete_queue->empty()) {
            sleep(10);
            continue;
        }
        struct BgJob* job = m_complete_queue->front();
        struct timeval current;
        gettimeofday(&current, NULL);
        if (current.tv_sec >= job->expire_ts.tv_sec) {
            m_complete_queue->pop_front();
            LOG_INFO << "bgjob new_vol:" << job->new_volume  << "reclaim";
            delete job;
        }
        sleep(60);
    }
}
