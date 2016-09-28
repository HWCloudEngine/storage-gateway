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

// TODO:GC thread to check and delete unwanted journal files and their metas, 
//and seal journals of crushed client
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
    parser.reset();
    lease_->init(access_key.c_str(),
            secret_key.c_str(),host.c_str(),bucket_name.c_str(),gc_interval);
    
    tick_ = 1;
    GC_window_ = 10; // TODO: config in config file?
    lease_check_window_ = 5;
    thread_GC_.reset(new ::std::thread([this]{
        int ticks = 0;
        while(GC_running_){
            std::this_thread::sleep_for(std::chrono::seconds(tick_));
            ticks++;
            if(ticks%GC_window_==0)
                GC_task();
            if(ticks % lease_check_window_ == 0)
                lease_check_task();
        }
    }));
    GC_running_ = true;
    LOG_INFO << "init GC thread, id=" << thread_GC_->get_id();
    return 0;
}

void GCTask::GC_task(){
    // TODO: journal GC
    LOG_DEBUG << "GC_task";
    update_volume_set();
    if(vols_.empty())
        return;
    for(auto it=vols_.begin();it!=vols_.end();++it){
        std::list<string> list;
        RESULT res = meta_ptr_->get_sealed_and_consumed_journals(*it,0,list);
        if(res != DRS_OK || list.empty())
            continue;
        res = meta_ptr_->recycle_journals(*it,list);
        if(res != DRS_OK){
            LOG_WARN << "recycle " << *it << "journals failed!";
            continue;
        }
    }
}

void GCTask::lease_check_task(){
    // TODO: check writers lease and sealed expired writer's journals
    LOG_DEBUG << "lease check";
    update_volume_set();
    if(vols_.empty())
        return;
    for(auto it=vols_.begin();it!=vols_.end();++it){
        std::list<string> list;
        RESULT res = meta_ptr_->get_producer_id(*it,list);
        if(res != DRS_OK)
            continue;
        if(list.empty())
            continue;
        for(auto list_it=list.begin();list_it!=list.end();++list_it){
            if(false == lease_->check_lease_existance(*list_it)){
                LOG_DEBUG << *list_it << " lease not existance";
                std::list<string> key_list;
                res = meta_ptr_->seal_opened_journals(*it,*list_it);
                DR_ASSERT(DRS_OK == res);
            }
        }
    }
}

int GCTask::add_volume(const std::string &vol_id){
    std::pair<std::set<string>::iterator,bool> ret;
    std::lock_guard<std::mutex> lck(add_mutex_);
    ret = vol_to_add_.insert(vol_id);
    if(ret.second == false)
        LOG_WARN << "add volume " << vol_id << " failed!";
    return 0;
}

int GCTask::remove_volume(const std::string &vol_id){
    std::pair<std::set<string>::iterator,bool> ret;
    std::lock_guard<std::mutex> lck(remove_mutex_);
    ret = vol_to_remove_.insert(vol_id);
    if(ret.second == false)
        LOG_WARN << "remove volume " << vol_id << " failed!";
    return 0;
}

void GCTask::update_volume_set(){
    if(!vol_to_add_.empty()){
        std::pair<std::set<string>::iterator,bool> ret;
        std::lock_guard<std::mutex> lck(add_mutex_);                
        for(auto it=vol_to_add_.begin();it!=vol_to_add_.end();it++){
            ret = vols_.insert(*it);
            if(ret.second==false)
                LOG_WARN << "volume " << *it << " was already added!";
        }
        vol_to_add_.clear();
    }
    if(!vol_to_remove_.empty()){
        std::lock_guard<std::mutex> lck(remove_mutex_);
        for(auto it=vol_to_remove_.begin();it!=vol_to_remove_.end();it++){
            auto it_ = vols_.find(*it);
            if(it_ != vols_.end())
                vols_.erase(it_);
            else
                LOG_WARN << "volume " << *it << " is not found in vol_map_!";
        }
        vol_to_remove_.clear();
    }
#if 1 // TODO: debug for single-point test, to delete when volume-id can be dynamiclly added
    std::list<string> list;
    RESULT res = meta_ptr_->list_volumes(list);
    if(res != DRS_OK || list.empty())
        return;
    std::lock_guard<std::mutex> lck(remove_mutex_);
    std::for_each(list.begin(),list.end(),[this,list](string s){
        vols_.insert(s);
        LOG_DEBUG << "add volume " << s;
    });
#endif
}
