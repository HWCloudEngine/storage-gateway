/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_receiver_marker.hpp
* Author: 
* Date:         2016/11/21
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_RECEIVER_MARKER_HPP_
#define REP_RECEIVER_MARKER_HPP_
#include <map>
#include <vector>
#include <atomic>
#include <thread>
#include <algorithm>
#include "rep_type.h"
#include "common/blocking_queue.h"
#include "log/log.h"
#include "rep_functions.hpp"
using std::string;
using huawei::proto::JournalMarker;
using huawei::proto::REPLICATER;

class RepReceiverMarker {
typedef struct ProducerMarker{
    uint64_t pre_id;
    JournalMarker producing_marker;
    std::vector<std::shared_ptr<RepTask>> tasks;
}ProducerMarker;
private:
    std::unique_ptr<BlockingQueue<std::shared_ptr<RepTask>>> writer_que_;
    bool done_;
    string uuid_;
    std::shared_ptr<CephS3Meta> meta_;
    std::map<string,std::unique_ptr<ProducerMarker>> volumes_;
    std::unique_ptr<std::thread> thread_;
public:
    void add_written_journal(std::shared_ptr<RepTask>& task){
        DR_ASSERT(true == writer_que_->push(task)); //blocking queue is thread-safe
    }
    void update_producing_marker(){
        while(!done_){
            std::shared_ptr<RepTask> task = writer_que_->pop();
            if(!task)
                continue;
            auto it = volumes_.find(task->vol_id);
            if(it == volumes_.end()){
                std::unique_ptr<ProducerMarker> marker(new ProducerMarker());
                RESULT res = meta_->get_journal_marker(uuid_,task->vol_id,REPLICATER,
                    &(marker->producing_marker),false);
                DR_ASSERT(res == DRS_OK);
                marker->pre_id = 0;
                volumes_.insert(std::pair<string,std::unique_ptr<ProducerMarker>>
                    (task->vol_id,std::move(marker)));
                it = volumes_.begin();
            }
            std::unique_ptr<ProducerMarker>& marker = it->second;
            std::vector<std::shared_ptr<RepTask>>& v = marker->tasks;
            v.push_back(task);
            std::sort(v.begin(),v.end());
            auto it1=v.begin();
            auto it2=it1;
            for(;it2!=v.end();it2++){
                if(marker->pre_id != (*it2)->seq_id){
                    // TODO: when drserver process restart, the seq id may not start at 0
                    if(marker->pre_id == 0
                        && marker->producing_marker.cur_journal()
                            .compare((*it2)->info->key) == 0
                        && marker->producing_marker.pos() >= (*it2)->info->pos){
                        marker->pre_id = (*it2)->seq_id;
                        LOG_INFO << "init seq id at " << (*it2)->info->key << ":"
                            << (*it2)->info->pos;
                    }
                    else
                        break;
                }
                marker->pre_id++;
            }
            if(it1==it2)
                continue;
            it2--;
            if(marker->producing_marker.cur_journal().compare((*it2)->info->key) < 0
                || (marker->producing_marker.cur_journal().compare((*it2)->info->key)==0
                    && marker->producing_marker.pos()<=(*it2)->info->end)){
                if(marker->producing_marker.cur_journal().compare((*it2)->info->key)!=0
                    || marker->producing_marker.pos()!=(*it2)->info->end){
                    marker->producing_marker.set_pos((*it2)->info->end);
                    marker->producing_marker.set_cur_journal((*it2)->info->key);
                    RESULT res = meta_->update_journal_marker(uuid_,task->vol_id,REPLICATER,
                        marker->producing_marker,false);
                    DR_ASSERT(res == DRS_OK);
                    LOG_INFO << "update producer marker " << (*it2)->info->key
                        << ":" << (*it2)->info->end;
                }
                v.erase(it1,++it2);
            }
            else{
                DR_ERROR_OCCURED();
            }
        }
    }

    RepReceiverMarker(const string& uuid,std::shared_ptr<CephS3Meta> meta):
        uuid_(uuid),
        meta_(meta),
        done_(false),
        writer_que_(new BlockingQueue<std::shared_ptr<RepTask>>()) {
            thread_.reset(new std::thread(&RepReceiverMarker::update_producing_marker,this));
        }
    ~RepReceiverMarker(){
        done_=true;
        if(thread_->joinable())
            thread_->join();
    }
};
#endif
