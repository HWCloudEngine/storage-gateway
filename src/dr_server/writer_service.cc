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

WriterServiceImpl::WriterServiceImpl(JournalMetaManager* meta) {
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
    //TODO:fix possible writer conflicts
    //request->uuid();
    LOG_DEBUG << "request vol:" << request->vol_id() << ", journal count:" << request->limits();
    std::list<string> list;
    RESULT result = _meta->get_volume_journals(request->vol_id(),request->limits(),list);
    if(result != DRS_OK) {
        reply->set_result(INTERNAL_ERROR);
        LOG_ERROR << "get volume " << request->vol_id() << "'s journals failed.";
    }
    else {
        reply->set_result(DRS_OK);
        for(std::list<string>::iterator it=list.begin(); it!=list.end(); ++it) {
            std::string *journal = reply->add_journals();
            *journal = *it;
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
    string journals[request->journals_size()];
    for(int i=0; i<request->journals_size(); ++i) {
        journals[i] = request->journals(i);
    }
    RESULT res = _meta->seal_volume_journals(request->vol_id(),journals,
            request->journals_size());
    if(res != DRS_OK){
        reply->set_result(INTERNAL_ERROR);
    }
    else {
        reply->set_result(DRS_OK);
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

