/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    markers_maintainer.cc
* Author: 
* Date:         2017/02/06
* Version:      1.0
* Description:
* 
************************************************/
#include "markers_maintainer.h"
#include "sg_server/sg_util.h"
#include "sg_server/transfer/net_sender.h"
#define MARKER_MAINTAINER_THREAD_COUNT (1) // should always be 1, to keep marker updating in order
using huawei::proto::transfer::ReplicateMarkerReq;
using huawei::proto::transfer::MessageType;
using huawei::proto::transfer::EncodeType;

void MarkersMaintainer::init(
        std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> in_que){
    in_que_ = in_que;
    tp_.reset(new sg_threads::ThreadPool(MARKER_MAINTAINER_THREAD_COUNT,
                MARKER_MAINTAINER_THREAD_COUNT));
    // start all thread in tp, run work methon in concurently
    for(int i=0; i<MARKER_MAINTAINER_THREAD_COUNT; i++){        
        tp_->submit(std::bind(&MarkersMaintainer::work,this));
    }
}

void MarkersMaintainer::work(){
    // init stream
    ClientContext* rpc_ctx = new ClientContext;
    grpc_stream_ptr stream;
    while(running_){
        stream = NetSender::instance().create_stream(rpc_ctx);
        if(nullptr == stream){
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }
        break;
    }
    LOG_INFO << "create grpc stream success!";

    // run sync marker tasks
    while(running_){
        // get a marker context
        std::shared_ptr<MarkerContext> ctx = in_que_->pop();
        SG_ASSERT(ctx != nullptr);
        if(nullptr == ctx->rep_ctx){
            LOG_WARN << "ReplicatorContext is null when try to sync markers";
            return;
        }


        // construct marker(only major include) for peer_voume:replace with peer volume
        JournalMarker temp_marker;
        uint64_t counter;
        SG_ASSERT(true == sg_util::extract_major_counter_from_journal_key(
            ctx->marker.cur_journal(),counter));
        string key = sg_util::construct_journal_key(
            ctx->rep_ctx->get_peer_volume(),counter);
        temp_marker.set_cur_journal(key);
        temp_marker.set_pos(ctx->marker.pos());
        // sync destination producer marker
        ReplicateMarkerReq marker_req;
        marker_req.set_vol_id(ctx->rep_ctx->get_peer_volume());
        marker_req.mutable_marker()->CopyFrom(temp_marker);
        string marker_req_str;
        SG_ASSERT(true == marker_req.SerializeToString(&marker_req_str));
        TransferRequest req;
        req.set_type(MessageType::REPLICATE_MARKER);
        req.set_encode(EncodeType::NONE_EN);
        req.set_data(marker_req_str.c_str(),marker_req_str.length());
        if(!stream->Write(req)){
            LOG_ERROR << "sync remote producer marker failed:"
                << temp_marker.cur_journal() << ":" << temp_marker.pos();
            continue;
        }
        else{
            TransferResponse res;
            if(stream->Read(&res)){ // blocked
                SG_ASSERT(res.id() == req.id());
                if(res.status()){
                    LOG_ERROR << "destination handle marker sync cmd failed!";
                    continue;
                }
            }
            else{
                LOG_ERROR << "replicate grpc read marker sync ack failed!";
                continue;
            }
        }

        // sync local volume consumer marker
        int up_ret = ctx->rep_ctx->update_consumer_marker(ctx->marker);
        if(up_ret){
            LOG_ERROR << "update [" << ctx->rep_ctx->get_vol_id()
                << "] replicator consumer marker failed:"
                << ctx->marker.cur_journal();
        }
        else{
            LOG_INFO << "update [" << ctx->rep_ctx->get_vol_id()
                << "] replicator consumer marker to:"
                << ctx->marker.cur_journal() << ":" << ctx->marker.pos();
        }
    }

    // recycle rpc resource
    if(rpc_ctx){
        delete rpc_ctx;
    }

    stream->WritesDone();
    Status status = stream->Finish();// may block if not all data in the stream were read
    if (!status.ok()) {
        LOG_ERROR << "replicate client close stream failed!";
    }
}

