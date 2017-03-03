/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_message_handlers.cc
* Author: 
* Date:         2017/03/02
* Version:      1.0
* Description:
* 
************************************************/
#include <algorithm>
#include "log/log.h"
#include "sg_server/sg_util.h"
#include "common/crc32.h"
#include "rep_message_handlers.h"
#include "common/config_parser.h"

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
using huawei::proto::VolumeMeta;
using huawei::proto::VolumeInfo;
using huawei::proto::REP_PRIMARY;
using huawei::proto::REP_FAILED_OVER;

RepMsgHandlers::RepMsgHandlers(std::shared_ptr<CephS3Meta> meta,
        const string& path):meta_(meta),
        mount_path_(path){
    // init lease client
    std::unique_ptr<ConfigParser> parser(new ConfigParser(DEFAULT_CONFIG_FILE));
    lease_client_.reset(new CephS3LeaseClient());
    std::string access_key,secret_key,host,bucket_name;
    access_key = parser->get_default("ceph_s3.access_key",access_key);
    secret_key = parser->get_default("ceph_s3.secret_key",secret_key);
    host = parser->get_default("ceph_s3.host",host);
    int renew_window = parser->get_default("ceph_s3.lease_renew_window",100);
    int expire_window = parser->get_default("ceph_s3.lease_expire_window",600);
    int validity_window = parser->get_default("ceph_s3.lease_validity_window",150);
    bucket_name = parser->get_default("ceph_s3.bucket",bucket_name);

    lease_client_->init(access_key.c_str(), secret_key.c_str(),
                       host.c_str(), bucket_name.c_str(), renew_window,
                       expire_window, validity_window) ;
}

RepMsgHandlers::~RepMsgHandlers(){
}

// TODO: reject sync io&marker if it's primary, or secondary with rep status  failedover rep status
StatusCode RepMsgHandlers::rep_handler(const TransferRequest& req){

    switch(req.type()){
        case MessageType::REPLICATE_DATA:
            hanlde_replicate_data_req(req);
            break;

        case MessageType::REPLICATE_MARKER:
            if(handle_replicate_marker_req(req))
                return (sOk);
            else
                return (sInternalError);

        case MessageType::REPLICATE_START:
            if(handle_replicate_start_req(req))
                return (sOk);
            else
                return (sInternalError);

        case MessageType::REPLICATE_END:
            // TODO: if some writes failed, set res=-1 to let client retry the task?
            if(handle_replicate_end_req(req))
                return (sOk);
            else
                return (sInternalError);

        default:
            break;
    }
    return StatusCode::sInvalidOperation;
}

bool RepMsgHandlers::handle_replicate_marker_req(const TransferRequest& req){
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

bool RepMsgHandlers::hanlde_replicate_data_req(const TransferRequest& req){
    // deserialize message from TransferRequest
    ReplicateDataReq data_msg;
    bool ret = data_msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);

    if(!validate_replicate(data_msg.vol_id())){
        LOG_ERROR << "the volume[" << data_msg.vol_id() << "] replicate was denied!";
        return false;
    }

    // get journal file fd && write data
    std::shared_ptr<std::ofstream> of = get_fstream(data_msg.vol_id(),
        data_msg.journal_counter());
    if(of == nullptr){
        LOG_INFO << "journal file not found, create it:"
            << data_msg.vol_id() << ":" << data_msg.journal_counter();
        of = create_journal(data_msg.vol_id(), data_msg.journal_counter());
        if(of == nullptr){
            LOG_ERROR << "create journal " << data_msg.vol_id()
                << ":" << data_msg.journal_counter() << " failed!";
            return ret;
        }
        // inset journal file to map
        const Jkey jkey(data_msg.vol_id(), data_msg.journal_counter());
        std::unique_lock<std::mutex> lock(mutex_);
        js_map_.insert(std::pair<const Jkey,std::shared_ptr<std::ofstream>>(jkey,of));
        lock.unlock();
    }

    if(data_msg.data().length() > 0){

        uint32_t crc = crc32c(data_msg.data().c_str(),data_msg.data().length(),0);
        LOG_DEBUG << "j_counter[" << data_msg.journal_counter()
            << "] receive data, len:" << data_msg.data().length()
            << ",offset:" << data_msg.offset() << ",crc:"
            << crc;

        of->seekp(data_msg.offset());
        of->write(data_msg.data().c_str(),data_msg.data().length());
        DR_ASSERT(of->fail()==0 && of->bad()==0);
    }
    return true;
}

bool RepMsgHandlers::handle_replicate_start_req(const TransferRequest& req){
    // TODO: pre-fetch journals?
    // deserialize message from TransferRequest
    ReplicateStartReq msg;
    bool ret = msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);
    if(!validate_replicate(msg.vol_id())){
        LOG_ERROR << "the volume[" << msg.vol_id() << "] replicate was denied!";
        return false;
    }
    // create journal
    std::shared_ptr<std::ofstream> of_p =
            create_journal(msg.vol_id(),msg.journal_counter());
    if(of_p == nullptr){
        LOG_ERROR << "create journal " << msg.vol_id()
            << ":" << msg.journal_counter() << " failed!";
        return false;
    }
    // inset journal file to map
    const Jkey jkey(msg.vol_id(),msg.journal_counter());
    std::lock_guard<std::mutex> lock(mutex_);
    js_map_.insert(std::pair<const Jkey,std::shared_ptr<std::ofstream>>(jkey,of_p));

    return true;
}

std::shared_ptr<std::ofstream> RepMsgHandlers::create_journal(
            const string& vol_id,const int64_t& counter){
    // create journal key&file 
    string key = sg_util::construct_journal_key(vol_id,counter);
    std::list<string> keys;
    keys.push_back(key);
    RESULT res = meta_->create_journals_by_given_keys(
        lease_client_->get_lease(),vol_id.c_str(),keys);
    if(res != DRS_OK){
        LOG_ERROR << "create_journals error!";
        return nullptr;
    }

    // get journal file path
    JournalMeta meta;
    res = meta_->get_journal_meta(key, meta);
    if(res != DRS_OK){
        LOG_ERROR << "get journal meta error!";
        return nullptr;
    }
    string path = mount_path_ + meta.path();
    // open journal file
    std::shared_ptr<std::ofstream> of_p(// if "in" open mode is not set, the file will be trancated?
        new std::ofstream(path.c_str(),std::ofstream::binary|std::ofstream::in));
    if(!of_p->is_open()){
        LOG_ERROR << "open journal file filed:" << path << ",key:" << keys.front();
        return nullptr;
    }

    return of_p;
}

bool RepMsgHandlers::handle_replicate_end_req(const TransferRequest& req){

    // deserialize message from TransferRequest
    ReplicateEndReq msg;
    bool ret = msg.ParseFromString(req.data());
    DR_ASSERT(ret == true);

    // close & fflush journal files and seal the journal
    std::shared_ptr<std::ofstream> of = get_fstream(msg.vol_id(),
        msg.journal_counter());
    if(of == nullptr){
        LOG_ERROR << "file[" << msg.vol_id() << ":"
            << msg.journal_counter() <<" not found!";
        return false;
    }

    if(of->is_open()){
        of->close();
    }
    // remove journal ofstream
    const Jkey jkey(msg.vol_id(),msg.journal_counter());
    std::unique_lock<std::mutex> lock(mutex_);
    js_map_.erase(jkey);
    lock.unlock();

    // source journal is opened, do not seal
    if(msg.is_open()){
        return true;
    }
    // seal the journal
    std::string key = sg_util::construct_journal_key(msg.vol_id(),msg.journal_counter());
    string keys_a[1]={key};
    RESULT res = meta_->seal_volume_journals(lease_client_->get_lease(),msg.vol_id(),keys_a,1);
    if(res != DRS_OK){
        return false;
    }

    return true;
}

std::shared_ptr<std::ofstream> RepMsgHandlers::get_fstream(const string& vol,
        const int64_t& counter){
    const Jkey key(vol,counter);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = js_map_.find(key);
    if(it == js_map_.end()){
        return nullptr;
    }
    return it->second;
}

bool RepMsgHandlers::validate_replicate(const string& vol_id){
    VolumeMeta meta;
    RESULT res = meta_->read_volume_meta(vol_id,meta);
    DR_ASSERT(res == DRS_OK);
    if(meta.info().role() == REP_PRIMARY)
        return false;
    else if(meta.info().rep_status() == REP_FAILED_OVER)
        return false;
    else
        return true;
}

