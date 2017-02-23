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
#include <stdlib.h>// posix_memalign
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
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<TransferRequest,TransferResponse>>
        stream = stub_->transfer(&context);// grpc stream should be created in work thread
    TransferRequest _req;
    int64_t id = 0;
    _req.set_id(id); 
    _req.set_vol_id(task->get_vol_id());
    _req.set_state(task->get_is_opened()? 1:0);
    _req.set_cmd(START_CMD);//start send flag
    _req.set_current_counter(task->get_j_counter());
    bool replicate_flag = true;
    // transfer start, notify dest to create journal
    do{
        if(!stream->Write(_req)){
            LOG_ERROR << "send replicate start cmd failed, state:"
                << get_printful_state(get_state(false));
            replicate_flag = false;
            break;
        }
        // wait for receiver ack
        TransferResponse res;
        if(stream->Read(&res)){
            DR_ASSERT(res.id() == _req.id());
            if(res.status()){
                LOG_ERROR << "destination start rep task failed, taskid: "
                    << task->get_id();
                replicate_flag = false;
                break;
            }
        }
        else{
            LOG_ERROR << "read response of rep start failed, rpc state:"
                << get_printful_state(get_state(false));
            replicate_flag = false;
            break;
        }
    }while(false);
    if(!replicate_flag){
        stream->WritesDone();
        stream->Finish();
        task->set_status(T_ERROR);
        return;
    }

    // transfer journal data
    task->set_status(T_RUNNING);
    size_t block_size = task->get_block_size();
    const size_t buf_len = block_size%512==0? block_size:((block_size>>9)+1)<<9;
    char* buffer = nullptr;
    if(posix_memalign((void**)(&buffer),512,buf_len) != 0){
        LOG_ERROR << "alloc memory failed,size=" << buf_len;
        task->set_status(T_ERROR);
        return;
    }
    LOG_DEBUG << "read block size:" << buf_len;
    TransferRequest req;
    req.set_vol_id(task->get_vol_id());
    req.set_cmd(DATA_CMD);
    while(task->has_next_block())
    {
        uint64_t offset;
        int64_t counter;
        int len = task->get_next_block(counter,buffer,offset,buf_len);
        if(len <= 0){
            LOG_ERROR << "task id[" << task->get_id()
                << "]:get next block failed:"
                << counter
                << ",offset=" << offset;
            replicate_flag = false;
            break;
        }
        req.set_id(++id);
        req.set_offset(offset);
        req.set_data(buffer,len);
        req.set_current_counter(counter);
        if(!stream->Write(req)){
            LOG_ERROR << "grpc write " << req.vol_id() << ":" 
                << req.current_counter() << ":" << req.offset() << " failed!";
            replicate_flag = false;
            break;
        }
    }
    if(buffer){
        free(buffer);
    }

    // notify destination that journal tranfered done, and close the grpc stream
    if(replicate_flag){ // no error
        _req.set_id(++id);
        _req.set_cmd(FINISH_CMD); // sent compeleted flag
        if(stream->Write(_req)){
            // wait for ack
            TransferResponse res;
            if(stream->Read(&res)){
                DR_ASSERT(res.id() == _req.id());
                if(res.status()){
                    replicate_flag = false;
                }
            }
            else{
                LOG_ERROR << "replicate client read error, state:"
                    << get_printful_state(get_state(false));
                replicate_flag = false;
            }
        }
        else{
            replicate_flag = false;
        }
    }
    stream->WritesDone();
    Status status = stream->Finish();// may block if not read all date in the stream
    if (!status.ok()) {
        replicate_flag = false;
        LOG_ERROR << "replicate client close stream failed!";
    }
    if(replicate_flag){
        task->set_status(T_DONE);
        // note: if failed, do not run callback function, or task window goes wrong
        task->get_callback()(task);
    }
    else{
        // set status at last step, or the task maybe resubmitted before return
        task->set_status(T_ERROR);
        LOG_ERROR << "run task[" << task->get_id() << "], key["
            << task->get_j_counter() << "] failed!";
    }
}

