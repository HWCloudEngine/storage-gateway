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

void MarkersMaintainer::wait_for_grpc_stream_ready(ClientContext* rpc_ctx,
        grpc_stream_ptr& stream){
    while(running_){
        stream = NetSender::instance().create_stream(rpc_ctx);
        if(nullptr == stream){
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }
        LOG_INFO << "create grpc stream success!";
        break;
    }
}


void MarkersMaintainer::work(){
    // init stream
    ClientContext* rpc_ctx = new ClientContext;
    grpc_stream_ptr stream;
    wait_for_grpc_stream_ready(rpc_ctx,stream);
    std::queue<std::shared_ptr<MarkerContext>> pending_que;

    // run sync marker tasks
    while(running_){
        // handle failed marker sync
        while(!pending_que.empty()){
            std::shared_ptr<MarkerContext>& ctx = pending_que.front();
            if(sync_marker(ctx,stream) == 0){
                pending_que.pop();
            }
            else{
                LOG_WARN << " re-create marker maintainer rpc stream...";
                stream->WritesDone();
                stream->Finish();
                if(rpc_ctx){
                    delete rpc_ctx;
                }
                rpc_ctx = new ClientContext;
                wait_for_grpc_stream_ready(rpc_ctx,stream);
            }
        }

        // get a marker context
        std::shared_ptr<MarkerContext> ctx = in_que_->pop();
        SG_ASSERT(ctx != nullptr);
        SG_ASSERT(nullptr != ctx->rep_ctx);
        int ret = sync_marker(ctx,stream);
        if(ret < 0){
            // recreate grcp stream & keep this markerContext and redo later
            stream->WritesDone();
            stream->Finish();
            if(rpc_ctx){
                delete rpc_ctx;
            }
            rpc_ctx = new ClientContext;
            wait_for_grpc_stream_ready(rpc_ctx,stream);
            pending_que.push(ctx);
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

int MarkersMaintainer::sync_marker(std::shared_ptr<MarkerContext>& ctx,
        grpc_stream_ptr& stream){
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
        return -1;
    }
    else{
        TransferResponse res;
        if(stream->Read(&res)){ // blocked
            SG_ASSERT(res.id() == req.id());
            if(res.status()){
                LOG_ERROR << "destination handle marker sync cmd failed!";
                return -1;
            }
        }
        else{
            LOG_ERROR << "replicate grpc read marker sync ack failed!";
            return -1;
        }
    }

    // sync local volume consumer marker
    int up_ret = ctx->rep_ctx->update_consumer_marker(ctx->marker);
    if(up_ret){
        LOG_ERROR << "update [" << ctx->rep_ctx->get_vol_id()
            << "] replicator consumer marker failed:"
            << ctx->marker.cur_journal();
        return -1;
    }
    else{
        LOG_INFO << "update [" << ctx->rep_ctx->get_vol_id()
            << "] replicator consumer marker to:"
            << ctx->marker.cur_journal() << ":" << ctx->marker.pos();
    }
    return 0;
}

