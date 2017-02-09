/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_transmitter.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include<fstream>
#include "rep_transmitter.h"
#include "log/log.h"
#include "../sg_util.h"
using std::string;
using grpc::Status;
using google::protobuf::int32;
using huawei::proto::transfer::START_CMD;
using huawei::proto::transfer::DATA_CMD;
using huawei::proto::transfer::FINISH_CMD;
#define TRANSMITTER_THREAD_COUNT (8)
std::string Transmitter::get_printful_state(ClientState state){
    string str;
    switch(state){
        case CLIENT_IDLE:
            str.append("idle");
            break;
        case CLIENT_CONNECTING:
            str.append("connecting");
            break;
        case CLIENT_READY:
            str.append("ready");
            break;
        case CLIENT_FAILURE:
            str.append("transient_failure");
            break;
        case CLIENT_SHUTDOWN:
            str.append("shutdown");
            break;
        default:
            break;
    }
    return str;
}

ClientState Transmitter::get_state(bool try_to_connect){
    return channel_->GetState(try_to_connect);
}

bool Transmitter::wait_for_state_change(const ClientState& state,
        std::chrono::system_clock::time_point deadline){
    return channel_->WaitForStateChange(state,deadline);
}

void Transmitter::init(std::shared_ptr<Channel> channel,
        std::shared_ptr<BlockingQueue<std::shared_ptr<RepTask>>> in,
        std::shared_ptr<BlockingQueue<std::shared_ptr<MarkerContext>>> out){
    channel_=(channel);
    stub_= std::move(huawei::proto::transfer::DataTransfer::NewStub(channel_));
    tp_.reset(new sg_threads::ThreadPool(TRANSMITTER_THREAD_COUNT,TRANSMITTER_THREAD_COUNT));
    running_ = true;
    seq_id_ = 0L;
    in_task_que_ = in;
    out_que_ = out;
    for(int i=0; i<TRANSMITTER_THREAD_COUNT; i++){
        tp_->submit(std::bind(&Transmitter::work,this));
    }
}

Transmitter::Transmitter(){
}
Transmitter::~Transmitter(){
    running_ = false;
}

int Transmitter::add_marker_context(ReplicatorContext* rep_ctx,
            const JournalMarker& marker){
    std::shared_ptr<MarkerContext> marker_ctx(new MarkerContext(rep_ctx,marker));
    if(out_que_->push(marker_ctx))
        return 0;
    else
        return -1;
}

bool Transmitter::sync_marker(const std::string& vol,const JournalMarker& marker){
    TransferRequest req;
    req.set_id(++seq_id_);
    req.set_vol_id(vol);
    int64_t c;
    if(!sg_util::extract_counter_from_object_key(marker.cur_journal(),c))
        return false;
    req.set_current_counter(c);
    req.set_offset(marker.pos());
    ClientContext context;
    TransferResponse res;
    Status status = stub_->sync_marker(&context,req,&res);
    if(status.ok() && !res.status())
        return true;
    else{
        LOG_ERROR << "sync marker failed, rpc status:" << status.error_message()
            << "; sync result:" << res.status();
        return false;
    }
}

void Transmitter::work(){
    std::function<void(Transmitter*,std::shared_ptr<RepTask>)> f
        = &Transmitter::do_transfer;
    while(running_){
        std::shared_ptr<RepTask> task = in_task_que_->pop();
        DR_ASSERT(task != nullptr);
        do_transfer(task);
    }
}
void Transmitter::do_transfer(std::shared_ptr<RepTask> task){
    const int BUF_LEN = 512000;
    char buffer[BUF_LEN];
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<TransferRequest,TransferResponse>>
        stream = stub_->transfer(&context);// grpc stream should be created in work thread
    TransferRequest _req;
    int64_t id = 0;
    _req.set_id(id); 
    _req.set_vol_id(task->vol_id);
    _req.set_state(task->info.is_opened? 1:0);
    _req.set_cmd(START_CMD);//start send flag
    int64_t c;
    DR_ASSERT(true == sg_util::extract_counter_from_object_key(task->info.key,c));
    _req.set_current_counter(c);
    _req.set_offset(task->info.pos);
    if(!stream->Write(_req)){
        LOG_ERROR << "send replicate start cmd failed, state:"
            << get_printful_state(get_state(false));
        task->status = T_ERROR;
        return;
    }
    // wait for receiver ack
    TransferResponse res;
    if(stream->Read(&res)){
        DR_ASSERT(res.id() == _req.id());
        if(res.status()){
            LOG_ERROR << "destination start rep task failed, taskid: "<< task->id;
            task->status = T_ERROR;
            return;
        }
    }
    else{
        LOG_ERROR << "read response of rep start failed, rpc state:"
            << get_printful_state(get_state(false));
        task->status = T_ERROR;
        return;
    }
    task->status = T_RUNNING;
    do{
        bool replicate_flag = true;
        std::ifstream is(task->info.path.c_str(), std::ifstream::binary);
        if(!is.is_open()){
            LOG_ERROR << "open file:" << task->info.path << " of "
                << task->info.key << " error.";
            task->status = T_ERROR;
            break;
        }
        is.seekg(task->info.pos);
        DR_ASSERT(is.fail()==0 && is.bad()==0);
        LOG_DEBUG << "replicating journal " << task->info.key << " from "
            << task->info.pos << " to " << task->info.end;
        TransferRequest req;
        int32 offset = task->info.pos;
        int64_t counter;
        DR_ASSERT(true == sg_util::extract_counter_from_object_key(task->info.key,counter));
        req.set_vol_id(task->vol_id);
        req.set_current_counter(counter);
        req.set_cmd(DATA_CMD);
        // TODO: if journal files were opened, verify checksum?
        // the replicate necessity of rest file data
        while (offset < task->info.end)
        {
            int len = offset+BUF_LEN < task->info.end ? BUF_LEN:(task->info.end-offset);
            if(is.read(buffer,len)){
                req.set_id(++id);
                req.set_offset(offset);
                req.set_data(buffer,len);
                if(!stream->Write(req)){
                    LOG_ERROR << "grpc write " << req.vol_id() << ":" 
                        << req.current_counter() << ":" << req.offset() << " failed!";
                    replicate_flag = false;
                    break;
                }
                offset += len;
            }
            else{
                if(!is.eof()){
                    LOG_ERROR << " read journal failed at(key:off:len) " 
                        << task->info.key << ":" << offset << ":" << len;
                    replicate_flag = false;
                    break;
                }
                if(is.gcount() > 0){
                    req.set_id(++id);
                    req.set_offset(offset);
                    req.set_data(buffer,is.gcount());
                    if(!stream->Write(req)){
                        LOG_ERROR << "grpc write " << req.vol_id() << ":" 
                            << req.current_counter() << ":" << req.offset() << " failed!";
                        replicate_flag = false;
                        break;
                    }
                    offset += is.gcount();
                }
                // TODO: set task failed and retry. if journal file created with zero padding,
                // the read should not failed of getting an EOF
                LOG_WARN << "read journal EOF at(key:off:len) " << task->info.key
                    << ":" << offset << ":" << len;
                break;
            }
        }
        // set postion, this is useful if the journal is in OPENED state
        if(task->info.is_opened)
            task->info.end = offset;
        LOG_DEBUG << task->info.key << " replicated at end:" << offset;
        is.close();
        if(!replicate_flag){
            task->status = T_ERROR;
            break;
        }
    }while(false);
    if(task->status == T_RUNNING){
        _req.set_id(++id);
        _req.set_offset(task->info.pos);
        _req.set_state(task->info.is_opened?1:0);
        _req.set_cmd(FINISH_CMD); // sent compeleted flag
        if(stream->Write(_req)){
            // wait for ack
            if(stream->Read(&res)){
                DR_ASSERT(res.id() == _req.id());
                if(res.status()){
                    task->status = T_ERROR;
                }
            }
            else{
                LOG_ERROR << "replicate client read error, state:"
                    << get_printful_state(get_state(false));
                task->status = T_ERROR;
            }
        }
        else{
            task->status = T_ERROR;
        }
    }
    stream->WritesDone();
    Status status = stream->Finish();// may block if not read all date in the stream
    if (!status.ok()) {
        task->status = T_ERROR;
        LOG_ERROR << "replicate client close stream failed!";
//        DR_ERROR_OCCURED();
    }
    if(task->status != T_ERROR){
        task->status = T_DONE;
        task->callback(task);
    }
}

