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

ConsumerServiceImpl::ConsumerServiceImpl(JournalMetaManager* meta){
    _meta = meta;
}

Status ConsumerServiceImpl::GetJournalMarker(ServerContext* context,
        const GetJournalMarkerRequest* request,
        GetJournalMarkerResponse* reply) {
    JournalMarker *marker = reply->mutable_marker();
    LOG_DEBUG << "get volume:" << request->vol_id() << " 's marker ";
    RESULT res = _meta->get_journal_marker(request->vol_id(),request->type(),marker);
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
    JournalMarker marker = request->marker();
    std::list<string> list;
    RESULT res = _meta->get_consumer_journals(request->vol_id(),marker,
            request->limit(), list);
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
    JournalMarker marker = request->marker();
    RESULT res = _meta->update_journals_marker(request->vol_id(),request->type(),marker);
    if(res != DRS_OK) {
        LOG_ERROR << "update " << request->vol_id() << " journals marker failed!";
        reply->set_result(INTERNAL_ERROR);
    }
    else {
        reply->set_result(DRS_OK);
    }    
    return Status::OK;
}
