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
using std::string;
using grpc::Status;
using google::protobuf::int64;
using google::protobuf::int32;

int64_t get_counter(const string& key){
    return std::stoll(key.substr(key.find_last_of('/')+1),nullptr);
}

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
        max_tasks_(max_tasks),
        running_tasks_(0){
}
int RepClient::submit_task(std::shared_ptr<RepTask> task,
        const std::function<void(std::shared_ptr<RepTask>)>& callback){
    if(running_tasks_ >= max_tasks_){
        LOG_ERROR << "submit failed,replicateing task queue is full!";
        return -1;
    }
    task->id = ++unique_id_;
    std::unique_lock<std::mutex> lck(mtx_);
    ++running_tasks_;
    task_map_.insert(std::pair<int,std::shared_ptr<RepTask>>(task->id,task));
    lck.unlock();
    task->send.reset(new std::thread(&RepClient::do_replicate,this,task,callback));
    LOG_DEBUG << "start_task tid " << task->id << ":" << std::hex << task->send->get_id()
        << std::dec;
    task->send->detach(); // TODO: use thread pool
    return task->id;
}

TASK_STATUS RepClient::query_task_status(int task_id){
    auto it = task_map_.find(task_id);
    if(it != task_map_.end()){ // TODO: delete task whose status is done
        return (it->second)->status;
    }
    else
        return T_UNKNOWN;
}

void RepClient::do_replicate(std::shared_ptr<RepTask> task,
        const std::function<void(std::shared_ptr<RepTask>)>& callback){
    const int BUF_LEN = 512000;
    char buffer[BUF_LEN];
    ClientContext context;
    task->stream = stub_->replicate(&context);// grpc stream should be created in work thread
    ReplicateRequest _req;
    int64_t id = 0;
    _req.set_id(id); 
    _req.set_vol_id(task->vol_id);
    _req.set_cmd(huawei::proto::START_CMD);// batch of journals start send flag
    for(JournalInfo info:*(task->journals.get())){
        _req.add_related_journals(get_counter(info.key));
    }
    if(!task->stream->Write(_req)){
        LOG_ERROR << "replicate client write error, state:"
            << get_printful_state(get_state(false));
        task->status = T_ERROR;
        return;
    }
    // wait for receiver ack
    ReplicateResponse res;
    if(task->stream->Read(&res)){
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
    for(JournalInfo& info:*(task->journals.get())){
        bool replicate_flag = true;
        std::ifstream is(info.path.c_str(), std::ifstream::binary);
        if(!is.is_open()){
            LOG_ERROR << "open file:" << info.path << " of " << info.key << " error.";
            task->status = T_ERROR;
            break;
        }
        is.seekg(info.pos);
        LOG_DEBUG << "open journal " << info.key << " at " << info.pos;
        ReplicateRequest req;
        int32 offset = 0;
        int64 counter = get_counter(info.key);
        req.set_vol_id(task->vol_id);
        req.set_current_counter(counter);
        req.set_cmd(huawei::proto::DATA_CMD);
        while (true)
        {
            if(is.read(buffer,BUF_LEN)){// TODO: how to avoid excessive reading if end marker is set??
                req.set_id(++id);
                req.set_offset(offset);
                req.set_data(buffer,BUF_LEN);
                if(!task->stream->Write(req)){
                    LOG_ERROR << "grpc write " << req.vol_id() << ":" 
                        << req.current_counter() << ":" << req.offset() << " failed!";
                    replicate_flag = false;
                    break;
                }
                offset += BUF_LEN;
            }
            else{
                if(is.gcount() > 0){
                    req.set_id(++id);
                    req.set_offset(offset);
                    req.set_data(buffer,is.gcount());
                    if(!task->stream->Write(req)){
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
        if(info.is_opened)
            info.end = offset;
        LOG_DEBUG << info.key << " replicated at pos:" << info.end;
        is.close();
        if(!replicate_flag){
            task->status = T_ERROR;
            break;
        }
    }
    if(task->status == T_RUNNING){
        _req.set_id(++id); // batch of journals sent compeleted flag
        _req.set_cmd(huawei::proto::FINISH_CMD);
        if(task->stream->Write(_req)){
            task->status = T_DONE;
            task->result = 0;// TODO:
            // wait for ack
            if(task->stream->Read(&res)){
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
    task->stream->WritesDone();
    Status status = task->stream->Finish();// may block if not read all date in the stream
    if (!status.ok()) {
        task->status = T_ERROR;
        LOG_ERROR << "replicate client finish stream failed!";
//        DR_ERROR_OCCURED();
    }
    std::unique_lock<std::mutex> lck(mtx_);
    running_tasks_--;
    task_map_.erase(task->id);
    lck.unlock();
    callback(task);
}

