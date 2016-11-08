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

bool get_path_by_journal_key(const std::list<string> &keys,
    std::vector<string>& paths){
    for(string key:keys){
        JournalMeta meta;
        RESULT res = JournalMetaHandle::instance().get_journal_meta(key,meta);
        DR_ASSERT(res == DRS_OK);
        paths.push_back(meta.path());
    }
    return true;
}

const string construct_journal_key(const string& vol_id,const int64& counter){
    string key("/journals/");
    key += vol_id;
    char temp[13];
    sprintf(temp,"%012lld",counter);
    key.append("/").append(temp);
    return key;
}

RepReceiver::RepReceiver(std::shared_ptr<CephS3Meta> meta,
        const string& path):meta_(meta),
        mount_path_(path){
    uuid_.append("dr_uuid");
}

Status RepReceiver::replicate(ServerContext* context, 
        ServerReaderWriter<ReplicateResponse,ReplicateRequest>* stream){
    static int g_thread_id = 0;
    int thread_id = ++g_thread_id;
    LOG_INFO << "replicate receiver id: " << thread_id;
    
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
    // seek? if source site send data block in senquece, it's not necessary
    of->seekp(req.offset());
    if(req.data().length() > 0)
        of->write(req.data().c_str(),req.data().length());
    return true;
}
bool RepReceiver::init_journals(const ReplicateRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    std::list<string> keys;
    for(int i=0; i<req.related_journals_size(); i++){
        keys.push_back(construct_journal_key(req.vol_id(),req.related_journals(i)));
    }
    RESULT res = meta_->create_journals_by_given_keys(uuid_.c_str(),
        req.vol_id().c_str(),keys);
    if(res != DRS_OK){
        LOG_ERROR << "create_journals error!";
        return false;
    }
    std::vector<string> paths;
    get_path_by_journal_key(keys,paths);
    for(int i=0;i<req.related_journals_size();i++){
        string path = mount_path_ + paths[i];
        std::shared_ptr<std::ofstream> of_p(new std::ofstream(path.c_str(),std::ifstream::binary));
        //of_p->seekp(0); // set write position
        const Jkey jkey(req.vol_id(),req.related_journals(i));
        js_map.insert(std::pair<const Jkey,std::shared_ptr<std::ofstream>>(jkey,of_p));
        std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
        LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
            << ":start write journal " 
            << req.vol_id() << ":" << req.related_journals(i) << std::endl;
    }
    return true;
}
bool RepReceiver::seal_journals(const ReplicateRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    std::list<string> keys;
    // close & fflush journal files and seal the journal
    for(int i=0;i<req.related_journals_size();i++){
        std::shared_ptr<std::ofstream> of = get_fstream(req.vol_id(),
            req.related_journals(i),js_map);
        if(of->is_open()){
            of->flush();
            of->close();
        }        
        // remove journal ofstream
        const Jkey key(req.vol_id(),req.related_journals(i));
        js_map.erase(key);
        // collect journal keys
        keys.push_back(construct_journal_key(req.vol_id(),req.related_journals(i)));
        std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
        LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
            << ":seal journal " 
            << req.vol_id() << ":" << req.related_journals(i);
    }
    // seal journals
    string keys_a[keys.size()];
    std::copy(keys.begin(),keys.end(),keys_a);
    RESULT res = meta_->seal_volume_journals(uuid_,req.vol_id(),keys_a,keys.size());
    if(res != DRS_OK){
        return false;
    }
    // TODO: update producing marker
    return true;
}

std::shared_ptr<std::ofstream> RepReceiver::get_fstream(const string& vol,
        const int64& counter,
        const std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    const Jkey key(vol,counter);
    auto it = js_map.find(key);
    assert(it != js_map.end());
    return it->second;
}

