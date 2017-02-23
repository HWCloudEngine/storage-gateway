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
#include "common/journal_meta_handle.h"
#include "../sg_util.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;
using google::protobuf::int64;
using google::protobuf::int32;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using huawei::proto::JournalMeta;
using huawei::proto::transfer::START_CMD;
using huawei::proto::transfer::DATA_CMD;
using huawei::proto::transfer::FINISH_CMD;
using huawei::proto::REPLAYER;
using huawei::proto::sOk;
using huawei::proto::sInternalError;
RepReceiver::RepReceiver(std::shared_ptr<CephS3Meta> meta,
        const string& path):meta_(meta),
        mount_path_(path){
    uuid_.append("dr_uuid");// TODO:
}

// TODO: reject sync io&marker if it's primary, or secondary with rep status  failedover rep status
Status RepReceiver::transfer(ServerContext* context, 
        ServerReaderWriter<TransferResponse,TransferRequest>* stream){
    static int g_stream_id = 0;
    int stream_id = ++g_stream_id;
    LOG_DEBUG << "replicate receiver stream id: " << stream_id;

    TransferRequest req;
    TransferResponse res;
    std::map<const Jkey,std::shared_ptr<std::ofstream>> js_map;
    while(stream->Read(&req)) {
        if(DATA_CMD == req.cmd()){
            write(req,std::ref(js_map));
        }
        else if(START_CMD == req.cmd()){ // create journal keys and journal files
            bool ret = init_journals(req,std::ref(js_map));
            res.set_id(req.id());
            if(ret)
                res.set_status(sOk);
            else
                res.set_status(sInternalError);
            if(!stream->Write(res)){
                LOG_ERROR << "replicate receiver response task start failed:"
                    << req.vol_id();
                break;
            }
        }
        else if(FINISH_CMD == req.cmd()){ // seal journals
            bool ret = seal_journals(req,std::ref(js_map));
            res.set_id(req.id());
            // TODO: if some writes failed, set res=-1 to let client retry the task?
            if(ret)
                res.set_status(sOk);
            else
                res.set_status(sInternalError);
            if(!stream->Write(res)){
                LOG_ERROR << "response of ending task failed:"
                    << req.vol_id();
                break;
            }
        }
    }
    return Status::OK;
}
grpc::Status RepReceiver::sync_marker(ServerContext* context,
        const TransferRequest* req,
        TransferResponse* res){
    LOG_DEBUG << "sync_marker " << req->vol_id() << "," << req->current_counter();
    int flag = 0;
    JournalMarker marker;
    do {
        string key = sg_util::construct_journal_key(req->vol_id(),req->current_counter());
        auto it = markers_.find(req->vol_id());
        if(it == markers_.end()){
            RESULT result = meta_->get_producer_marker(req->vol_id(),REPLAYER,
                marker);
            if(NO_SUCH_KEY == result){ // not init, need update
                marker.set_pos(req->offset());
                marker.set_cur_journal(key);
                flag = 1;
                break;
            }
            else if(DRS_OK != result){
                LOG_ERROR << "get " << req->vol_id() << " producing marker failed!";
                flag = -1;
                break;
            }
        }

        if(marker.cur_journal().compare(key) < 0
            || (marker.cur_journal().compare(key)==0 && marker.pos() < req->offset())){
            marker.set_pos(req->offset());
            marker.set_cur_journal(key);
            flag = 1; // need to update to a bigger marker
            break;
        }
        else{
            flag = 0;
            LOG_WARN << "producer marker " << key << ":" << req->offset()
                << " not updated, " << marker.cur_journal() << ":" << marker.pos();
            break;
        }
    }while(false);
    if(flag < 0){ // an error occered
        res->set_status(sInternalError);
    }
    else if(flag > 0){ // update marker and update cache
        RESULT result = meta_->set_producer_marker(req->vol_id(),marker);
        DR_ASSERT(result == DRS_OK);
        auto it = markers_.find(req->vol_id());
        if(it == markers_.end()){
            markers_.insert(std::pair<std::string,JournalMarker>(req->vol_id(),marker));
        }
        else{
            it->second.CopyFrom(marker);
        }
        LOG_INFO << "update producer marker " << marker.cur_journal()
            << ":" << marker.pos();
        res->set_status(sOk);
    }
    else{ // no need to update marker
        res->set_status(sOk);
    }
    return Status::OK;
}

bool RepReceiver::write(const TransferRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    std::shared_ptr<std::ofstream>of = get_fstream(req.vol_id(),
        req.current_counter(),js_map);
    of->seekp(req.offset());
    if(req.data().length() > 0){
        of->write(req.data().c_str(),req.data().length());
        DR_ASSERT(of->fail()==0 && of->bad()==0);
    }
    return true;
}
bool RepReceiver::init_journals(const TransferRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    // TODO: pre-fetch journals?
    std::list<string> keys;
    keys.push_back(sg_util::construct_journal_key(req.vol_id(),req.current_counter()));
    RESULT res = meta_->create_journals_by_given_keys(uuid_.c_str(),
        req.vol_id().c_str(),keys);
    if(res != DRS_OK){
        LOG_ERROR << "create_journals error!";
        return false;
    }
    std::string path;
    sg_util::get_path_by_journal_key(keys.front(),path);
    path = mount_path_ + path;
    std::shared_ptr<std::ofstream> of_p(// if "in" open mode is not set, the file will be trancated?
        new std::ofstream(path.c_str(),std::ofstream::binary|std::ofstream::in));
    if(!of_p->is_open()){
        LOG_ERROR << "open journal file filed:" << path << ",key:" << keys.front();
        return false;
    }
    //of_p->seekp(0); // set write position
    const Jkey jkey(req.vol_id(),req.current_counter());
    js_map.insert(std::pair<const Jkey,std::shared_ptr<std::ofstream>>(jkey,of_p));
    std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
    LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
        << ":start task " 
        << req.vol_id() << ":" << req.current_counter() << std::endl;
    return true;
}
bool RepReceiver::seal_journals(const TransferRequest& req,
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

    if(req.state()){ // journal is opened, do not seal
        return true;
    }

    std::string key = sg_util::construct_journal_key(req.vol_id(),req.current_counter());
    if(req.state() == 0){
        string keys_a[1]={key};
        RESULT res = meta_->seal_volume_journals(uuid_,req.vol_id(),keys_a,1);
        if(res != DRS_OK){
            return false;
        }
    }
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

