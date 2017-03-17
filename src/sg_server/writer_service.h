/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    writer_service.h
* Author: 
* Date:         2016/07/12
* Version:      1.0
* Description:
* 
**********************************************/
#ifndef WRITER_SERVICE_H_
#define WRITER_SERVICE_H_
#include <memory>
#include <grpc++/grpc++.h>
#include "rpc/writer.grpc.pb.h"
#include "journal_meta_manager.h"

using grpc::Status;
using grpc::ServerContext;
using huawei::proto::Writer;
using huawei::proto::GetWriteableJournalsRequest;
using huawei::proto::GetWriteableJournalsResponse;
using huawei::proto::SealJournalsRequest;
using huawei::proto::SealJournalsResponse;
using huawei::proto::GetMultiWriteableJournalsRequest;
using huawei::proto::GetMultiWriteableJournalsResponse;
using huawei::proto::SealMultiJournalsRequest;
using huawei::proto::SealMultiJournalsResponse;
using huawei::proto::UpdateProducerMarkerRequest;
using huawei::proto::UpdateProducerMarkerResponse;
using huawei::proto::UpdateMultiProducerMarkersRequest;
using huawei::proto::UpdateMultiProducerMarkersResponse;

class WriterServiceImpl final : public Writer::Service {
private:
    std::shared_ptr <JournalMetaManager> _meta;
public:
    WriterServiceImpl(std::shared_ptr<JournalMetaManager> meta);
    Status GetWriteableJournals(ServerContext* context,
            const GetWriteableJournalsRequest* request,
            GetWriteableJournalsResponse* reply);
    Status SealJournals(ServerContext* context,
            const SealJournalsRequest* request,
            SealJournalsResponse* response);
    Status UpdateProducerMarker(ServerContext* context,
            const UpdateProducerMarkerRequest* request,
            UpdateProducerMarkerResponse* response);
    Status GetMultiWriteableJournals(ServerContext* context, 
            const GetMultiWriteableJournalsRequest* request,
            GetMultiWriteableJournalsResponse* response);
    Status SealMultiJournals(ServerContext* context,
            const SealMultiJournalsRequest* request,
            SealMultiJournalsResponse* response);
    Status UpdateMultiProducerMarkers(ServerContext* context,
            const UpdateMultiProducerMarkersRequest* request,
            UpdateMultiProducerMarkersResponse* response);
};

#endif
