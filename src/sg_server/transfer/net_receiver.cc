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
#include "net_receiver.h"
#include "log/log.h"
#include "common/config_parser.h"
#include "common/journal_meta_handle.h"
#include "sg_server/sg_util.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;
using google::protobuf::int64;
using google::protobuf::int32;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using huawei::proto::JournalMeta;
using huawei::proto::transfer::MessageType;
using huawei::proto::transfer::EncodeType;
using huawei::proto::transfer::ReplicateDataReq;
using huawei::proto::transfer::ReplicateMarkerReq;
using huawei::proto::transfer::ReplicateStartReq;
using huawei::proto::transfer::ReplicateEndReq;
using huawei::proto::REPLAYER;
using huawei::proto::StatusCode;
using huawei::proto::sOk;
using huawei::proto::sInternalError;

NetReceiver::NetReceiver(std::shared_ptr<CephS3Meta> meta,
        const string& path):meta_(meta),
        mount_path_(path){
    uuid_.append("dr_uuid");// TODO:
}

// TODO: reject sync io&marker if it's primary, or secondary with rep status  failedover rep status
Status NetReceiver::transfer(ServerContext* context, 
        ServerReaderWriter<TransferResponse,TransferRequest>* stream){
    static int g_stream_id = 0;
    int stream_id = ++g_stream_id;
    LOG_DEBUG << "replicate receiver stream id: " << stream_id;

    TransferRequest req;
    TransferResponse res;
    std::map<const Jkey,std::shared_ptr<std::ofstream>> js_map;
    while(stream->Read(&req)) {
        switch(req.type()){
            case MessageType::REPLICATE_DATA:
                hanlde_replicate_data_req(req,std::ref(js_map));
                break;
            case MessageType::REPLICATE_MARKER:
                res.set_id(req.id());
                if(handle_replicate_marker_req(req))
                    res.set_status(sOk);
                else
                    res.set_status(sInternalError);
                if(!stream->Write(res)){
                    LOG_ERROR << "replicate receiver handle task start failed:"
                        << req.id();
                }
                break;
            case MessageType::REPLICATE_START:
                res.set_id(req.id());
                if(handle_replicate_start_req(req,std::ref(js_map)))
                    res.set_status(sOk);
                else
                    res.set_status(sInternalError);
                if(!stream->Write(res)){
                    LOG_ERROR << "replicate receiver handle task start failed:"
                        << req.id();
                }
                break;
            case MessageType::REPLICATE_END:
                res.set_id(req.id());
                // TODO: if some writes failed, set res=-1 to let client retry the task?
                if(handle_replicate_end_req(req,std::ref(js_map)))
                    res.set_status(sOk);
                else
                    res.set_status(sInternalError);
                if(!stream->Write(res)){
                    LOG_ERROR << "response of ending task failed:"
                        << req.id();
                }
                break;

            default:
                break;
        }
    }
    return Status::OK;
}

bool NetReceiver::handle_replicate_marker_req(const TransferRequest& req){
    // deserialize message from TransferRequest
    ReplicateMarkerReq msg;
    bool ret = msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);

    LOG_DEBUG << "sync_marker, volume=" << msg.vol_id() << ",marker="
        << msg.marker().cur_journal() << ":" << msg.marker().pos();

    // get replayer producer marker, if failed, try to update it
    JournalMarker marker;
    RESULT result = meta_->get_producer_marker(msg.vol_id(),REPLAYER,
                marker);
    if(result == DRS_OK){
        // compare the markers, if the one sent is bigger, update it
        int cmp = sg_util::marker_compare(msg.marker(),marker);
        if(cmp <= 0){
            LOG_WARN << "the new producer marker "
                << msg.marker().cur_journal() << ":" << msg.marker().pos()
                << " is less than the last " 
                << marker.cur_journal() << ":" << marker.pos();
            return true;
        }
    }
    result = meta_->set_producer_marker(msg.vol_id(),msg.marker());
    DR_ASSERT(result == DRS_OK);
    LOG_INFO << "update replayer producer marker to: "
        << msg.marker().cur_journal() << ":" << msg.marker().pos();
    return true;
}

bool NetReceiver::hanlde_replicate_data_req(const TransferRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    // deserialize message from TransferRequest
    ReplicateDataReq data_msg;
    bool ret = data_msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);

    // get journal file fd && write data
    std::shared_ptr<std::ofstream> of = get_fstream(data_msg.vol_id(),
        data_msg.journal_counter(),js_map);
    if(of == nullptr){
        LOG_INFO << "journal file not found, create it:"
            << data_msg.vol_id() << data_msg.journal_counter();
        ret = create_journal(data_msg.vol_id(), data_msg.journal_counter(),js_map);
        if(ret != true)
            return ret;
        of = get_fstream(data_msg.vol_id(), data_msg.journal_counter(),js_map);
        DR_ASSERT(of != nullptr);
    }
    of->seekp(data_msg.offset());
    if(data_msg.data().length() > 0){
        of->write(data_msg.data().c_str(),data_msg.data().length());
        DR_ASSERT(of->fail()==0 && of->bad()==0);
    }
    return true;
}

bool NetReceiver::handle_replicate_start_req(const TransferRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    // TODO: pre-fetch journals?
    // deserialize message from TransferRequest
    ReplicateStartReq msg;
    bool ret = msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);
    // create journal
    return create_journal(msg.vol_id(),msg.journal_counter(),js_map);
}

bool NetReceiver::create_journal(const string& vol_id,const int64_t& counter,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    string key = sg_util::construct_journal_key(vol_id,counter);
    std::list<string> keys;
    keys.push_back(key);
    RESULT res = meta_->create_journals_by_given_keys(uuid_.c_str(),
        vol_id.c_str(),keys);
    if(res != DRS_OK){
        LOG_ERROR << "create_journals error!";
        return false;
    }

    // get journal file path
    JournalMeta meta;
    res = meta_->get_journal_meta(key, meta);
    if(res != DRS_OK){
        LOG_ERROR << "get journal meta error!";
        return false;
    }
    string path = mount_path_ + meta.path();
    // open journal file
    std::shared_ptr<std::ofstream> of_p(// if "in" open mode is not set, the file will be trancated?
        new std::ofstream(path.c_str(),std::ofstream::binary|std::ofstream::in));
    if(!of_p->is_open()){
        LOG_ERROR << "open journal file filed:" << path << ",key:" << keys.front();
        return false;
    }

    // inset journal file to map
    const Jkey jkey(vol_id,counter);
    js_map.insert(std::pair<const Jkey,std::shared_ptr<std::ofstream>>(jkey,of_p));
    std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
    LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
        << ":start task " 
        << vol_id << ":" << counter << std::endl;
    return true;
}

bool NetReceiver::handle_replicate_end_req(const TransferRequest& req,
        std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
     // TODO:seal journals within indipendent thread

    // deserialize message from TransferRequest
    ReplicateEndReq msg;
    bool ret = msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);

    // close & fflush journal files and seal the journal
    std::shared_ptr<std::ofstream> of = get_fstream(msg.vol_id(),
        msg.journal_counter(),js_map);
    if(of->is_open()){
        of->close();
    }
    // remove journal ofstream
    const Jkey jkey(msg.vol_id(),msg.journal_counter());
    js_map.erase(jkey);
    std::chrono::system_clock::duration dtn = std::chrono::system_clock::now().time_since_epoch();
    LOG_DEBUG << std::chrono::duration_cast<std::chrono::seconds>(dtn).count()
        << ":end task " 
        << msg.vol_id() << ":" << msg.journal_counter();

    // journal is opened, do not seal
    if(msg.is_open()){
        return true;
    }
    // seal the journal
    std::string key = sg_util::construct_journal_key(msg.vol_id(),msg.journal_counter());
    string keys_a[1]={key};
    RESULT res = meta_->seal_volume_journals(uuid_,msg.vol_id(),keys_a,1);
    if(res != DRS_OK){
        return false;
    }

    return true;
}

std::shared_ptr<std::ofstream> NetReceiver::get_fstream(const string& vol,
        const int64& counter,
        const std::map<const Jkey,std::shared_ptr<std::ofstream>>& js_map){
    const Jkey key(vol,counter);
    auto it = js_map.find(key);
    if(it == js_map.end()){
        return nullptr;
    }
    return it->second;
}

