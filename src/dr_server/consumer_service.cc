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
#include "consumer_service.h"
using huawei::proto::JournalMarker;

Status ReplayerServiceImpl::GetJournalMarker(ServerContext* context, const GetJournalMarkerRequest* request,
            GetJournalMarkerResponse* reply) {
    JournalMarker *marker = reply->mutable_marker();
    std::cout << "get volume:" << request->vol_id() << " 's marker " << std::endl;
    reply->set_result(huawei::proto::DRS_OK);
    std::string s = "journals/vol_1/journal_1";
    marker->set_cur_journal(s);
    marker->set_pos(0);

    return Status::OK;
}
