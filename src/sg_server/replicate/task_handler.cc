/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    task_handler.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include<fstream>
#include "task_handler.h"
#include "log/log.h"
#include "../sg_util.h"
#include <stdlib.h>// posix_memalign
using std::string;
using grpc::Status;
using google::protobuf::int32;
using huawei::proto::transfer::MessageType;

#define TRANSMITTER_THREAD_COUNT (8)

void TaskHandler::init(
        std::shared_ptr<BlockingQueue<std::shared_ptr<TransferTask>>> in,
        std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> out){
    tp_.reset(new sg_threads::ThreadPool(TRANSMITTER_THREAD_COUNT,TRANSMITTER_THREAD_COUNT));
    running_ = true;
    seq_id_ = 0L;
    in_task_que_ = in;
    out_que_ = out;
    for(int i=0; i<TRANSMITTER_THREAD_COUNT; i++){
        ClientContext* ctx = new ClientContext;
        grpc_stream_ptr stream = NetSender::instance().create_stream(ctx);
        tp_->submit(std::bind(&TaskHandler::work,this,ctx,std::ref(stream)));
    }
}

TaskHandler::TaskHandler(){
}
TaskHandler::~TaskHandler(){
    running_ = false;
}

int TaskHandler::add_marker_context(ReplicatorContext* rep_ctx,
            const JournalMarker& marker){
    std::shared_ptr<MarkerContext> marker_ctx(new MarkerContext(rep_ctx,marker));
    if(out_que_->push(marker_ctx))
        return 0;
    else
        return -1;
}

void TaskHandler::work(ClientContext* ctx, grpc_stream_ptr& stream){
    while(running_){
        std::shared_ptr<TransferTask> task = in_task_que_->pop();
        DR_ASSERT(task != nullptr);
        do_transfer(task,stream);
    }
    if(ctx){
        delete ctx;
    }

    stream->WritesDone();
    Status status = stream->Finish();// may block if not all data in the stream were read
    if (!status.ok()) {
        LOG_ERROR << "replicate client close stream failed!";
    }
}

int handle_replicate_cmd(TransferRequest* req,
        grpc_stream_ptr& stream){
    if(stream->Write(*req)){
        TransferResponse res;
        if(stream->Read(&res)){ // blocked
            DR_ASSERT(res.id() == req->id());
            if(!res.status()){
                return 0;
            }
            LOG_ERROR << "destination handle replicate cmd failed!";
        }
        else{
            LOG_ERROR << "replicate grpc read failed!";
        }
    }
    else{
        LOG_ERROR << "replicate grpc write failed!";
        return -1;
    }
    return -1;
}

void TaskHandler::do_transfer(std::shared_ptr<TransferTask> task,
                grpc_stream_ptr& stream){
    LOG_DEBUG << "start process transfer task, id=" << task->get_id();

    while(task->has_next_package()){
        TransferRequest* req = task->get_next_package();
        DR_ASSERT(req != nullptr);
        switch(req->type()){
            case MessageType::REPLICATE_DATA:
                if(!stream->Write(*req)){
                    LOG_ERROR << "send replicate data failed!, task id:"
                        << task->get_id();
                    task->set_status(T_ERROR);
                }
                break;
            case MessageType::REPLICATE_START:
                if(handle_replicate_cmd(req,stream) != 0){
                    LOG_ERROR << "handle replicate start req failed!, task id:"
                        << task->get_id();
                    task->set_status(T_ERROR);
                }
                break;
            case MessageType::REPLICATE_END:
                if(handle_replicate_cmd(req,stream) != 0){
                    LOG_ERROR << "handle replicate end req failed!, task id:"
                        << task->get_id();
                    task->set_status(T_ERROR);
                }
                else{
                    task->set_status(T_DONE);
                    // note: if failed, do not run callback function, 
                    // or task window goes wrong
                    std::shared_ptr<RepContext> rep_ctx =
                        std::dynamic_pointer_cast<RepContext>(task->get_context());
                    rep_ctx->get_callback()(task);
                }
                break;


            default:
                LOG_WARN << "unknown transfer message, type=" << req->type()
                    << ", id=" << req->id();
                break;
        }
        // recycle req
        delete req;
    }
}

