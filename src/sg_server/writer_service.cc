/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    writer_service.cc
* Author: 
* Date:         2016/07/12
* Version:      1.0
* Description:
* 
**********************************************/
#include <grpc++/grpc++.h>
#include "writer_service.h"
#include "log/log.h"
using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

WriterServiceImpl::WriterServiceImpl(std::shared_ptr<JournalMetaManager> meta) {
    _meta = meta;
}

Status WriterServiceImpl::GetWriteableJournals(ServerContext* context,
        const GetWriteableJournalsRequest* request,
        GetWriteableJournalsResponse* reply){
    if(_meta == nullptr){
        LOG_ERROR << "journal meta is not init.";
        Status status(grpc::INTERNAL,"journal meta is not init.");
        return status;
    }
    // TODO:confirm uuid validity
    //request->uuid();
    LOG_DEBUG << "request vol:" << request->vol_id() << ", journal count:"
        << request->limits();
    std::list<JournalElement> list;
    RESULT result = _meta->create_journals(request->uuid(),request->vol_id(),
        request->limits(),list);
    if(result != DRS_OK) {
        reply->set_result(INTERNAL_ERROR);
        LOG_ERROR << "get volume " << request->vol_id() << "'s journals failed.";
    }
    else {
        reply->set_result(DRS_OK);
        for(std::list<JournalElement>::iterator it=list.begin(); it!=list.end(); ++it) {
            JournalElement *journal = reply->add_journals();
            journal->CopyFrom(*it);
        }
    }
    return Status::OK;
}

Status WriterServiceImpl::SealJournals(ServerContext* context,
        const SealJournalsRequest* request,
        SealJournalsResponse* reply) {
    if(_meta == nullptr){
        LOG_ERROR << "journal meta is not init.";
        Status status(grpc::INTERNAL,"journal meta is not init.");
        return status;
    }
     // TODO:confirm uuid validity
//     request->uuid()
    string journals[request->journals_size()];
    for(int i=0; i<request->journals_size(); ++i) {
        journals[i] = request->journals(i);
    }
    RESULT res = _meta->seal_volume_journals(request->uuid(),request->vol_id(),
        journals, request->journals_size());
    if(res != DRS_OK){
        reply->set_result(INTERNAL_ERROR);
    }
    else {
        reply->set_result(DRS_OK);
    }
    return Status::OK;
}

Status WriterServiceImpl::UpdateProducerMarker(ServerContext* context,
        const UpdateProducerMarkerRequest* request,
        UpdateProducerMarkerResponse* response){
    if(_meta == nullptr){
        LOG_ERROR << "journal meta is not init.";
        Status status(grpc::INTERNAL,"journal meta is not init.");
        return status;
    }
    // TODO: confirm uuid validity
    const string& uuid = request->uuid();
    const string& vol_id = request->vol_id();
    const JournalMarker& marker = request->marker();

    RESULT res = _meta->set_producer_marker(vol_id,marker);
    if(res != DRS_OK){
        response->set_result(INTERNAL_ERROR);
    }
    else {
        response->set_result(DRS_OK);
    }
    return Status::OK;
}

Status WriterServiceImpl::UpdateMultiProducerMarkers(ServerContext* context,
            const UpdateMultiProducerMarkersRequest* request,
            UpdateMultiProducerMarkersResponse* response){
    if(_meta == nullptr){
        LOG_ERROR << "journal meta is not init.";
        Status status(grpc::INTERNAL,"journal meta is not init.");
        return status;
    }
    // TODO: confirm uuid validity
    const string& uuid = request->uuid();
    auto& map = request->markers();

    response->set_result(DRS_OK);
    for(auto it=map.begin();it!=map.end();it++){
        RESULT res = _meta->set_producer_marker(it->first,it->second);
        if(res != DRS_OK){
            response->set_result(INTERNAL_ERROR);
        }
    }
    return Status::OK;
}

Status WriterServiceImpl::GetMultiWriteableJournals(ServerContext* context, 
        const GetMultiWriteableJournalsRequest* request,
        GetMultiWriteableJournalsResponse* reply) {
    // TODO
    Status status(grpc::UNIMPLEMENTED,"not implemented!");
    return status;
}

Status WriterServiceImpl::SealMultiJournals(ServerContext* context,
        const SealMultiJournalsRequest* request,
        SealMultiJournalsResponse* reply) {
    // TODO
    Status status(grpc::UNIMPLEMENTED,"not implemented!");
    return status;
}

