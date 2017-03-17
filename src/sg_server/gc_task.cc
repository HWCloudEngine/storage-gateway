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
#include "sg_util.h"
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;
using huawei::proto::REPLAYER;
using huawei::proto::REPLICATOR;

int GCTask::get_least_marker(const string& vol,std::set<IConsumer*>& set,
            JournalMarker& marker){
    bool valid = false;
    for(auto& c:set){
        if(nullptr != c){
            if(valid){
                JournalMarker temp;
                if(c->get_consumer_marker(temp) == 0){
                    if(j_meta_ptr_->compare_marker(marker,temp) > 0)
                        marker.CopyFrom(temp);
                }
            }
            else{
                JournalMarker temp;
                if(c->get_consumer_marker(temp) == 0){
                    marker.CopyFrom(temp);
                    valid = true;
                }
            }
        }
    }
    if(valid)
        return 0;
    else
        return -1;
}

int GCTask::init(std::shared_ptr<JournalGCManager> gc_meta,
            std::shared_ptr<JournalMetaManager> j_meta){
    std::lock_guard<std::mutex> lck(mtx_);
    if(GC_running_){
        LOG_ERROR << "gc task is already running!";
        return -1;
    }
    gc_meta_ptr_ = gc_meta;
    j_meta_ptr_ = j_meta;
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
    // since consumers(replayer/replicator) were registered seperately,
    // GC thread should wait until all consumers registered at init
    if(!volumes_initialized)
        return;
    if(vols_.empty())
        return;
    std::lock_guard<std::mutex> lck(mtx_);
    for(auto it=vols_.begin();it!=vols_.end();++it){
        auto& set = it->second;
        if(set.empty())
            continue;
        JournalMarker marker;
        if(0 != get_least_marker(it->first,set,marker))
            continue;

        auto& vol = it->first;
        std::list<string> list;
        RESULT res = gc_meta_ptr_->get_sealed_and_consumed_journals(vol,
            marker,0,list); // set limit=0 to get all matched journals
        if(res != DRS_OK || list.empty())
            continue;
        res = gc_meta_ptr_->recycle_journals(vol,list);
        if(res != DRS_OK){
            LOG_WARN << "recycle " << vol << " journals failed!";
            continue;
        }
    }
}

void GCTask::lease_check_task(){
    if(vols_.empty())
        return;
    std::lock_guard<std::mutex> lck(mtx_);
    for(auto it=vols_.begin();it!=vols_.end();++it){
        std::list<string> list;
        RESULT res = gc_meta_ptr_->get_producer_id(it->first,list);
        if(res != DRS_OK)
            continue;
        if(list.empty())
            continue;
        for(auto list_it=list.begin();list_it!=list.end();++list_it){
            if(list_it->compare(g_replicator_uuid) == 0) // ignore replicator uuid
                continue;
            if(false == lease_->check_lease_existance(*list_it)){
                LOG_DEBUG << *list_it << " lease not existance";
                res = gc_meta_ptr_->seal_opened_journals(it->first,*list_it);
                SG_ASSERT(DRS_OK == res);
            }
        }
    }
}

int GCTask::add_volume(const std::string &vol_id){
    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it != vols_.end()){
        LOG_WARN << "add failed, volume[" << vol_id << "] was already in gc";
        return -1;
    }
    std::set<IConsumer*> consumer_set;
    auto ret = vols_.insert(vc_pair_t(vol_id,consumer_set));
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

void GCTask::register_consumer(const string& vol_id,IConsumer* c){
    SG_ASSERT(c != nullptr);
    LOG_INFO << "register consumer," << vol_id << ":" << c->get_type();

    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it == vols_.end()){
        LOG_WARN << "register failed, no volume[" << vol_id << "] in gc";
        return;
    }
    auto& set = it->second;
    for(auto it2=set.begin();it2!=set.end();++it2){
        if((*it2)->get_type() == c->get_type()){
            LOG_ERROR << "register failed,volume[" << vol_id
                << "] consumer type[" << c->get_type() << "] exsit!";
            return;
        }
    }
    it->second.insert(c);
}

void GCTask::unregister_consumer(const string& vol_id,const CONSUMER_TYPE& type){
    std::lock_guard<std::mutex> lck(mtx_);
    auto it = vols_.find(vol_id);
    if(it == vols_.end()){
        LOG_WARN << "unregister failed, no volume[" << vol_id << "] in gc";
        return;
    }
    auto& set = it->second;
    for(auto it2=set.begin();it2!=set.end();++it2){
        if((*it2)->get_type() == type){
            if(REPLAYER == type) // REPLICATOR consumer use smart pointer, no need to delete memory
                delete (*it2);
            set.erase(it2);
            return;
        }
    }
    LOG_ERROR << "unregister failed,volume[" << vol_id
        << "] consumer type[" << type << "] not found!";
}

void GCTask::set_volumes_initialized(bool f){
    volumes_initialized = f;
}

void GCTask::clean_up(){
    std::lock_guard<std::mutex> lck(mtx_);
    for(auto it=vols_.begin();it!=vols_.end();it++){
        auto& set = it->second;
        for(auto it2=set.begin();it2!=set.end();++it2){
            if((*it2)->get_type() == REPLAYER){
                delete (*it2);
            }
        }
        set.clear();
    }
    vols_.clear();
}