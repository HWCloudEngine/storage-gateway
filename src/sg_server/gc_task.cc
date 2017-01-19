/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    gc_task.cc
* Author: 
* Date:         2016/09/07
* Version:      1.0
* Description:  recycle comsumed journals and seal the journals of crashed writers
* 
************************************************/
#include <chrono>
#include "gc_task.h"
#include "common/config_parser.h"
#include "log/log.h"
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::CONSUMER_NONE;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATOR;
using huawei::proto::CONSUMER_BOTH;
int GCTask::init(std::shared_ptr<JournalGCManager> meta){
    std::lock_guard<std::mutex> lck(mtx_);
    if(GC_running_){
        LOG_ERROR << "gc task is already running!";
        return -1;
    }
    meta_ptr_=(meta);
    int gc_interval = 1*60*60;  // TODO:config in config file
    lease_.reset(new CephS3LeaseServer());
    string access_key;
    string secret_key;
    string host;
    string bucket_name;
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    if(false == parser->get<string>("ceph_s3.access_key",access_key)){
        LOG_FATAL << "config parse ceph_s3.access_key error!";
        return INTERNAL_ERROR;
    }
    if(false == parser->get<string>("ceph_s3.secret_key",secret_key)){
        LOG_FATAL << "config parse ceph_s3.secret_key error!";
        return INTERNAL_ERROR;
    }
    // port number is necessary if not using default 80/443
    if(false == parser->get<string>("ceph_s3.host",host)){
        LOG_FATAL << "config parse ceph_s3.host error!";
        return INTERNAL_ERROR;
    }
    if(false == parser->get<string>("ceph_s3.bucket",bucket_name)){
        LOG_FATAL << "config parse ceph_s3.bucket error!";
        return INTERNAL_ERROR;
    }
    lease_check_window_ = parser->get_default<int>("ceph_s3.expire_window",10);
    GC_window_ = parser->get_default<int>("ceph_s3.gc_window",100);
    parser.reset();
    lease_->init(access_key.c_str(),
            secret_key.c_str(),host.c_str(),bucket_name.c_str(),gc_interval);
    tick_ = 1;
    GC_running_ = true;
    thread_GC_.reset(new ::std::thread([this]{
        int ticks = 0;
        while(GC_running_){
            std::this_thread::sleep_for(std::chrono::seconds(tick_));
            ticks++;
            if(ticks%GC_window_ == 0){
                do_GC();
            }
            if(ticks%lease_check_window_ == 0){
                lease_check_task();
            }
        }
    }));
    LOG_INFO << "init GC thread, id=" << thread_GC_->get_id();
    return 0;
}

void GCTask::do_GC(){
    if(vols_.empty())
        return;
    for(auto it=vols_.begin();it!=vols_.end();++it){
        std::list<string> list;
        RESULT res = meta_ptr_->get_sealed_and_consumed_journals(it->first,
            it->second,0,list);
        if(res != DRS_OK || list.empty())
            continue;
        res = meta_ptr_->recycle_journals(it->first,list);
        if(res != DRS_OK){
            LOG_WARN << "recycle " << it->first << " journals failed!";
            continue;
        }
    }
}

void GCTask::lease_check_task(){
    if(vols_.empty())
        return;
    for(auto it=vols_.begin();it!=vols_.end();++it){
        std::list<string> list;
        RESULT res = meta_ptr_->get_producer_id(it->first,list);
        if(res != DRS_OK)
            continue;
        if(list.empty())
            continue;
        for(auto list_it=list.begin();list_it!=list.end();++list_it){
            if(false == lease_->check_lease_existance(*list_it)){
                LOG_DEBUG << *list_it << " lease not existance";
                std::list<string> key_list;
                res = meta_ptr_->seal_opened_journals(it->first,*list_it);
                DR_ASSERT(DRS_OK == res);
            }
        }
    }
}

int GCTask::add_volume(const std::string &vol_id,const CONSUMER_TYPE& type){
    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it != vols_.end()){
        LOG_WARN << "add failed, volume[" << vol_id << "] was already in gc";
        return -1;
    }
    std::pair<std::map<string,CONSUMER_TYPE>::iterator,bool> ret;
    ret = vols_.insert(std::pair<string,CONSUMER_TYPE>(vol_id,type));
    if(ret.second == false){
        LOG_ERROR << "add volume[" << vol_id << "] to gc failed!";
        return -1;
    }
    return 0;
}

int GCTask::remove_volume(const std::string &vol_id){
    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it == vols_.end()){
        LOG_WARN << "remove failed, no volume[" << vol_id << "] in gc";
        return 0;
    }
    vols_.erase(it);
    return 0;
}

void merge_consumer_type(CONSUMER_TYPE& t1,const CONSUMER_TYPE& t2){
    switch(t1){
        case CONSUMER_NONE:
            t1 = t2;
            break;
        case CONSUMER_BOTH:
            break;
        case REPLAYER:
            if(CONSUMER_BOTH == t2 || REPLICATOR == t2)
                t1 = CONSUMER_BOTH;
            break;
        case REPLICATOR:
            if(CONSUMER_BOTH == t2 || REPLAYER == t2)
                t1 = CONSUMER_BOTH;
            break;
        default:
            break;
    }
    return ;
}
void shrink_consumer_type(CONSUMER_TYPE& t1,const CONSUMER_TYPE& t2){
    switch(t1){
        case CONSUMER_NONE:
            break;
        case CONSUMER_BOTH:
            if(REPLAYER == t2)
                t1 = REPLICATOR;
            else if(REPLICATOR == t2)
                t1 = REPLAYER;
            else if(CONSUMER_BOTH == t2)
                t1 = CONSUMER_NONE;
            break;
        case REPLAYER:
            if(REPLAYER == t2 || CONSUMER_BOTH == t2)
                t1 = CONSUMER_NONE;
            break;
        case REPLICATOR:
            if(REPLICATOR == t2 || CONSUMER_BOTH == t2)
                t1 = CONSUMER_NONE;
            break;
        default:
            break;
    }
    return ;
}

void GCTask::register_(const string& vol_id,const CONSUMER_TYPE& type){
    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it == vols_.end()){
        LOG_WARN << "register failed, no volume[" << vol_id << "] in gc";
    }
    merge_consumer_type(it->second,type);
}
void GCTask::unregister(const string& vol_id,const CONSUMER_TYPE& type){
    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it == vols_.end()){
        LOG_WARN << "unregister failed, no volume[" << vol_id << "] in gc";
    }
    shrink_consumer_type(it->second,type);
}

