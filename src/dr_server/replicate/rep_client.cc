/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_client.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include<fstream>
#include "rep_client.h"
#include "log/log.h"
#include "rep_functions.hpp"
using std::string;
using grpc::Status;
using google::protobuf::int32;
std::string RepClient::get_printful_state(ClientState state){
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

ClientState RepClient::get_state(bool try_to_connect){
    return channel_->GetState(try_to_connect);
}

bool RepClient::wait_for_state_change(const ClientState& state,
        std::chrono::system_clock::time_point deadline){
    return channel_->WaitForStateChange(state,deadline);
}
RepClient::RepClient(std::shared_ptr<Channel> channel,int max_tasks):
        unique_id_(0),
        channel_(channel),
        stub_(huawei::proto::Replicator::NewStub(channel)),
        task_pool_(new sg_threads::ThreadPool(max_tasks)){
}
int RepClient::submit_task(std::shared_ptr<RepTask> task,
        const std::function<void(std::shared_ptr<RepTask>)>& callback){
    std::unique_lock<std::mutex> lck(mtx_);
    task->id = ++unique_id_;
    lck.unlock();
    std::function<void(RepClient*,std::shared_ptr<RepTask>,
        const std::function<void(std::shared_ptr<RepTask>)>&)> f
        = &RepClient::do_replicate;
    if(!task_pool_->submit(std::bind(f,this,task,callback))){
        task->status = T_ERROR;
        return -1;
    }
    return task->id;
}

void RepClient::do_replicate(std::shared_ptr<RepTask> task,
        const std::function<void(std::shared_ptr<RepTask>)>& callback){
    const int BUF_LEN = 512000;
    char buffer[BUF_LEN];
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<ReplicateRequest,ReplicateResponse>>
        stream = stub_->replicate(&context);// grpc stream should be created in work thread
    ReplicateRequest _req;
    int64_t id = 0;
    _req.set_id(id); 
    _req.set_vol_id(task->vol_id);
    _req.set_state(task->info->is_opened? 1:0);
    _req.set_cmd(huawei::proto::START_CMD);//start send flag
    _req.set_current_counter(replicate::get_counter(task->info->key));
    _req.set_offset(task->info->pos);
    if(!stream->Write(_req)){
        LOG_ERROR << "replicate client write error, state:"
            << get_printful_state(get_state(false));
        task->status = T_ERROR;
        return;
    }
    // wait for receiver ack
    ReplicateResponse res;
    if(stream->Read(&res)){
        DR_ASSERT(res.id() == _req.id());
        DR_ASSERT(res.res() == 0);
    }
    else{
        LOG_ERROR << "replicate client read error, state:"
            << get_printful_state(get_state(false));
        task->status = T_ERROR;
        return;
    }
    task->status = T_RUNNING;
    do{
        bool replicate_flag = true;
        std::ifstream is(task->info->path.c_str(), std::ifstream::binary);
        if(!is.is_open()){
            LOG_ERROR << "open file:" << task->info->path << " of "
                << task->info->key << " error.";
            task->status = T_ERROR;
            break;
        }
        is.seekg(task->info->pos);
        DR_ASSERT(is.fail()==0 && is.bad()==0);
        LOG_DEBUG << "replicating journal " << task->info->key << " from "
            << task->info->pos << " to " << task->info->end;
        ReplicateRequest req;
        int32 offset = task->info->pos;
        int64 counter = replicate::get_counter(task->info->key);
        req.set_vol_id(task->vol_id);
        req.set_current_counter(counter);
        req.set_cmd(huawei::proto::DATA_CMD);
        // TODO: if journal files were created with zero padding, we should check
        // the replicate necessity of rest file data
        while (offset < task->info->end)
        {
            int len = offset+BUF_LEN < task->info->end ? BUF_LEN:(task->info->end-offset);
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
                break;
            }
        }
        // set postion, this is useful if the journal is in OPENED state
        if(task->info->is_opened)
            task->info->end = offset;
        LOG_DEBUG << task->info->key << " replicated at end:" << offset;
        is.close();
        if(!replicate_flag){
            task->status = T_ERROR;
            break;
        }
    }while(false);
    if(task->status == T_RUNNING){
        _req.set_id(++id);
        _req.set_offset(task->info->pos);
        _req.set_len(task->info->end - task->info->pos);
        _req.set_seq_id(task->seq_id);
        _req.set_cmd(huawei::proto::FINISH_CMD); // sent compeleted flag
        if(stream->Write(_req)){
            task->status = T_DONE;
            // wait for ack
            if(stream->Read(&res)){
                DR_ASSERT(res.id() == _req.id());
                DR_ASSERT(res.res() == 0);
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
        LOG_ERROR << "replicate client finish stream failed!";
//        DR_ERROR_OCCURED();
    }
    callback(task);
}

