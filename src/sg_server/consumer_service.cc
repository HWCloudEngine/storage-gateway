/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    consumer_service.cc
* Author: 
* Date:         2016/07/06
* Version:      1.0
* Description:
* 
**********************************************/
#include <memory>
#include <iostream>
#include "consumer_service.h"
#include "log/log.h"
using huawei::proto::JournalMarker;
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

ConsumerServiceImpl::ConsumerServiceImpl(std::shared_ptr<JournalMetaManager> meta){
    _meta = meta;
}
RESULT ConsumerServiceImpl::get_journal_marker(const std::string& vol,
        const CONSUMER_TYPE& type,
        JournalMarker& marker){
    return _meta->get_consumer_marker(vol,type,marker);
}
RESULT ConsumerServiceImpl::get_journal_list(const std::string& vol,
        const JournalMarker& marker,
        const int& limit,
        std::list<std::string> list,
        const CONSUMER_TYPE& type){
    return _meta->get_consumable_journals(vol,marker,limit,list,type);
}
RESULT ConsumerServiceImpl::update_consumer_marker(
        const std::string& vol,
        const CONSUMER_TYPE& type,
        const JournalMarker& marker){
    return _meta->update_consumer_marker(vol,type,marker);
}

Status ConsumerServiceImpl::GetJournalMarker(ServerContext* context,
        const GetJournalMarkerRequest* request,
        GetJournalMarkerResponse* reply) {
    JournalMarker *marker = reply->mutable_marker();
    const std::string& uuid = request->uuid();
    const std::string& vol = request->vol_id();
    const CONSUMER_TYPE& type = request->type();
    // TODO:confirm uuid validity
    LOG_DEBUG << "get volume:" << request->vol_id() << " 's marker ";
    RESULT res = get_journal_marker(vol,type,*marker);
    if(res != DRS_OK) {
        LOG_ERROR << "get " << request->vol_id() << " consumer marker failed!";
        reply->set_result(INTERNAL_ERROR);
    }
    else {
        reply->set_result(DRS_OK);
    }
    return Status::OK;
}

Status ConsumerServiceImpl::GetJournalList(ServerContext* context,
        const GetJournalListRequest* request,
        GetJournalListResponse* reply) {
    const JournalMarker marker = request->marker();
    std::list<string> list;
    const std::string& uuid = request->uuid();
    const std::string& vol = request->vol_id();
    const CONSUMER_TYPE& type = request->type();
    const int limit = request->limit();
    // TODO:confirm uuid validity
    RESULT res = get_journal_list(vol,marker,limit,list,type);
    if(res != DRS_OK) {
        LOG_ERROR << "get " << request->vol_id() << " consumer journals failed!";
        reply->set_result(INTERNAL_ERROR);
    }
    else {
        reply->set_result(DRS_OK);
        for(auto it=list.begin();it!=list.end();++it)
            reply->add_journals(*it);
    }    
    return Status::OK;
}

Status ConsumerServiceImpl::UpdateConsumerMarker(ServerContext* context,
        const UpdateConsumerMarkerRequest* request,
        UpdateConsumerMarkerResponse* reply) {
    const JournalMarker& marker = request->marker();
    const std::string& uuid = request->uuid();
    const std::string& vol = request->vol_id();
    const CONSUMER_TYPE& type = request->type();
    // TODO:confirm uuid validity
    RESULT res = update_consumer_marker(vol,type,marker);
    if(res != DRS_OK) {
        LOG_ERROR << "update " << request->vol_id() << " journals marker failed!";
        reply->set_result(INTERNAL_ERROR);
    }
    else {
        reply->set_result(DRS_OK);
    }
    return Status::OK;
}
