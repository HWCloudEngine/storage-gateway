/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_receiver.cc
* Author: 
* Date:         2016/10/21
* Version:      1.0
* Description:
* 
************************************************/
#include <algorithm>
#include "rep_receiver.h"
#include "log/log.h"
#include "common/config_parser.h"
#include "common/journal_meta_handle.hpp"
#include "rep_functions.hpp"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;
using huawei::proto::ReplicateRequest;
using huawei::proto::ReplicateResponse;
using google::protobuf::int64;
using google::protobuf::int32;
using huawei::proto::Replicator;
using huawei::proto::ReplicateResponse;
using huawei::proto::ReplicateRequest;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using huawei::proto::JournalMeta;

RepReceiver::RepReceiver(std::shared_ptr<CephS3Meta> meta,
        const string& path):meta_(meta),
        mount_path_(path){
    uuid_.append("dr_uuid");// TODO:
    markers_.reset(new RepReceiverMarker(uuid_,meta));
}

Status RepReceiver::replicate(ServerContext* context, 
        ServerReaderWriter<ReplicateResponse,ReplicateRequest>* stream){
    static int g_stream_id = 0;
    int stream_id = ++g_stream_id;
    LOG_DEBUG << "replicate receiver stream id: " << stream_id;

    ReplicateRequest req;
    ReplicateResponse res;
    std::map<const Jkey,std::shared_ptr<std::ofstream>> js_map;
    while(stream->Read(&req)) {
        if(huawei::proto::DATA_CMD == req.cmd()){
            write(req,std::ref(js_map));
        }
        else if(huawei::proto::START_CMD == req.cmd()){ // create journal keys and journal files
            bool ret = init_journals(req,std::ref(js_map));
            res.set_id(req.id());
            if(ret)
                res.set_res(0);
            else
                res.set_res(-1);
            if(!stream->Write(res)){
                LOG_ERROR << "replicate receiver response task start failed:"
                    << req.vol_id();
                break;
            }
        }
        else if(huawei::proto::FINISH_CMD == req.cmd()){ // seal journals
            bool ret = seal_journals(req,std::ref(js_map));
            res.set_id(req.id());
            // TODO: if some writes failed, set res=-1 to let client retry the task?
            if(ret)
                res.set_res(0);
            else
                res.set_res(-1);
            if(!stream->Write(res)){
                LOG_ERROR << "replicate receiver response task end failed:"
                    << req.vol_id();
                break;
            }
        }
    }
    return Status::OK;
}

bool RepReceiver::write(const ReplicateRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    std::shared_ptr<std::ofstream>of = get_fstream(req.vol_id(),
        req.current_counter(),js_map);
    of->seekp(req.offset());
    if(req.data().length() > 0)
        of->write(req.data().c_str(),req.data().length());
    return true;
}
bool RepReceiver::init_journals(const ReplicateRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    // TODO: pre-fetch journals?
    std::list<string> keys;
    keys.push_back(replicate::construct_journal_key(req.vol_id(),req.current_counter()));
    RESULT res = meta_->create_journals_by_given_keys(uuid_.c_str(),
        req.vol_id().c_str(),keys);
    if(res != DRS_OK){
        LOG_ERROR << "create_journals error!";
        return false;
    }
    std::string path;
    replicate::get_path_by_journal_key(keys.front(),path);
    path = mount_path_ + path;
    std::shared_ptr<std::ofstream> of_p(// if "in" open mode is not set, the file will be trancated?
        new std::ofstream(path.c_str(),std::ofstream::binary|std::ofstream::in));
    //of_p->seekp(0); // set write position
    const Jkey jkey(req.vol_id(),req.current_counter());
    js_map.insert(std::pair<const Jkey,std::shared_ptr<std::ofstream>>(jkey,of_p));
    std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
    LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
        << ":start task " 
        << req.vol_id() << ":" << req.current_counter() << std::endl;
    return true;
}
bool RepReceiver::seal_journals(const ReplicateRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
     // TODO:seal journals within indipendent thread
    // close & fflush journal files and seal the journal
    std::shared_ptr<std::ofstream> of = get_fstream(req.vol_id(),
        req.current_counter(),js_map);
    if(of->is_open()){
        of->close();
    }        
    // remove journal ofstream
    const Jkey jkey(req.vol_id(),req.current_counter());
    js_map.erase(jkey);
    std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
    LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
        << ":end task " 
        << req.vol_id() << ":" << req.current_counter();
    std::string key = replicate::construct_journal_key(req.vol_id(),req.current_counter());
    if(req.state() == 0){
        string keys_a[1]={key};
        RESULT res = meta_->seal_volume_journals(uuid_,req.vol_id(),keys_a,1);
        if(res != DRS_OK){
            return false;
        }
    }
    // TODO: update producing marker
    std::shared_ptr<RepTask> task(new RepTask());
    task->vol_id = req.vol_id();
    task->info.reset(new JournalInfo());
    task->info->key = key;
    task->info->pos = req.offset();
    task->info->end = req.offset() + req.len();
    task->seq_id = req.seq_id();
    markers_->add_written_journal(task);
    return true;
}

std::shared_ptr<std::ofstream> RepReceiver::get_fstream(const string& vol,
        const int64& counter,
        const std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    const Jkey key(vol,counter);
    auto it = js_map.find(key);
    DR_ASSERT(it != js_map.end());
    return it->second;
}

