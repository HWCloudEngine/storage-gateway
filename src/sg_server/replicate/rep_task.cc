/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_task.cc
* Author: 
* Date:         2017/02/16
* Version:      1.0
* Description:
* 
************************************************/
#include "rep_task.h"
#include "log/log.h"
#include "../sg_util.h"
#include "../snap_reader.h"
#include "rpc/message.pb.h"
#include "journal/journal_entry.h"
#include "journal/journal_type.h"
using huawei::proto::StatusCode;
using huawei::proto::transfer::MessageType;
using huawei::proto::transfer::EncodeType;
using huawei::proto::transfer::ReplicateStartReq;
using huawei::proto::transfer::ReplicateEndReq;
using huawei::proto::transfer::ReplicateDataReq;
using google::protobuf::Message;
using huawei::proto::WriteMessage;
using huawei::proto::DiskPos;
#define JOURNAL_BLOCK_SIZE (512*1024L)

int construct_journal_entry(JournalEntry& entry, 
                    const char* data, const uint64_t& off, const size_t& len){
    std::shared_ptr<WriteMessage> message(new WriteMessage);
    DiskPos* pos = message->add_pos();
    pos->set_offset(off);
    pos->set_length(len);
    message->set_data(data,len);
    entry.set_message(message);
    entry.set_type(IO_WRITE);
    entry.serialize();
    entry.calculate_crc();
    return 0;
}

int construct_transfer_data_request(TransferRequest* req,
            const char* buffer,
            const size_t& size,
            const uint64_t& offset,
            std::shared_ptr<RepContext> ctx,
            const uint64_t& id){
    req->set_id(id);
    req->set_encode(EncodeType::NONE_EN);
    req->set_type(MessageType::REPLICATE_DATA);
    ReplicateDataReq data_msg;
    data_msg.set_vol_id(ctx->get_vol_id());
    data_msg.set_journal_counter(ctx->get_j_counter());
    data_msg.set_offset(offset);
    data_msg.set_data(buffer,size);
    string temp;
    DR_ASSERT(true == data_msg.SerializeToString(&temp));
    req->set_data(temp.c_str(),temp.length());
    return 0;
}

int construct_transfer_start_request(TransferRequest* req,
            std::shared_ptr<RepContext> ctx,
            const uint64_t& id){
    req->set_id(id);
    req->set_encode(EncodeType::NONE_EN);
    req->set_type(MessageType::REPLICATE_START);
    ReplicateStartReq start_msg;
    start_msg.set_vol_id(ctx->get_vol_id());
    start_msg.set_journal_counter(ctx->get_j_counter());
    string temp;
    DR_ASSERT(true == start_msg.SerializeToString(&temp));
    req->set_data(temp.c_str(),temp.length());
    return 0;
}

int construct_transfer_end_request(TransferRequest* req,
            std::shared_ptr<RepContext> ctx,
            const uint64_t& id){
    req->set_id(id);
    req->set_encode(EncodeType::NONE_EN);
    req->set_type(MessageType::REPLICATE_END);
    ReplicateEndReq end_msg;
    end_msg.set_vol_id(ctx->get_vol_id());
    end_msg.set_journal_counter(ctx->get_j_counter());
    end_msg.set_is_open(ctx->get_is_open());
    string temp;
    DR_ASSERT(true == end_msg.SerializeToString(&temp));
    req->set_data(temp.c_str(),temp.length());
    return 0;
}

int JournalTask::init(){
    package_id = 0;
    DR_ASSERT(ctx != nullptr);
    is.open(path,std::ifstream::binary);
    if(!is.is_open()){
        LOG_ERROR << "open file:" << path << " of "
            << ctx->get_j_counter() << " error.";
        return -1;
    }
    cur_off = start_off;
    is.seekg(cur_off);
    DR_ASSERT(is.fail()==0 && is.bad()==0);
    LOG_DEBUG << "transfering journal " << ctx->get_j_counter() << " from "
        << start_off << " to " << ctx->get_end_off();

    buffer = (char*)malloc(JOURNAL_BLOCK_SIZE);
    if(buffer == nullptr){
        LOG_ERROR << "alloc buffer for journal task failed!";
        return -1;
    }
    return 0;
}

bool JournalTask::has_next_package(){
    return (!end);
}

// TODO: startReq
TransferRequest* JournalTask::get_next_package(){
    if(cur_off >= ctx->get_end_off()){
        TransferRequest* req = new TransferRequest;
        construct_transfer_end_request(req,ctx,++package_id);
        end = true;
        return req;
    }

    size_t size = (ctx->get_end_off()-cur_off) > JOURNAL_BLOCK_SIZE ?
        JOURNAL_BLOCK_SIZE:(ctx->get_end_off() - cur_off);
    if(is.read(buffer,size)){

    }
    else if(is.eof() && is.gcount()>0){
        size = is.gcount();
        // TODO: remove this line if file created with padding filled
        ctx->set_end_off(cur_off + size);
        LOG_DEBUG << "journal file[" << path << "] end at:" << cur_off + size;
    }
    else{
        LOG_ERROR << "read file[" << path << "] failed,required length["
            << size << "].";
        return nullptr;
    }

    TransferRequest* req = new TransferRequest;
    construct_transfer_data_request(req,buffer,size,cur_off,
        ctx,++package_id);
    cur_off += size; // update offset
    return req;
}


int DiffSnapTask::init(){
    diff_blocks.clear();
    vector_cursor = 0;
    array_cursor = 0;
    all_data_sent = false;
    cur_off = 0;
    package_id = 0;
    max_journal_size = MAX_JOURNAL_SIZE_FOR_SNAP_SYNC; // TODO

    // init diff_blocks
    StatusCode ret = SnapReader::instance().get_client()->DiffSnapshot(
            ctx->get_vol_id(),pre_snap,cur_snap,diff_blocks);
    DR_ASSERT(ret == StatusCode::sOk);
    if(diff_blocks.empty()){
        LOG_INFO << "there was no diff block in task[" << this->get_id()
                << "]";
        end = true;
        return 0;
    }
    else{
        if(diff_blocks[0].diff_block_no_size()<=0){
            LOG_INFO << "there was no diff block in task[" << this->get_id()
                << "]";
            end = true;
            return 0;
        }
    }

    buffer = (char*)malloc(COW_BLOCK_SIZE);
    if(buffer == nullptr){
        LOG_ERROR << "alloc buffer for diff block failed!";
        return -1;
    }
    return 0;
}

bool DiffSnapTask::has_next_package(){
    return (!end);
}

TransferRequest* DiffSnapTask::get_next_package(){
    if(!has_next_package()){
        return nullptr;
    }
    if(all_data_sent){ // all data sent
        TransferRequest* req = new TransferRequest;
        construct_transfer_end_request(req,ctx,++package_id);
        end = true;
        return req;
    }
    // fill journal file header
    if(cur_off == 0){
        // write journal file header:
        TransferRequest* req = new TransferRequest;
        journal_file_header_t header;
        header.magic = 0;
        header.version = 0;
        header.reserve = 0;
        size_t size = sizeof(journal_file_header_t);
        construct_transfer_data_request(req,(char*)(&header),size,cur_off,
            ctx,++package_id);
        cur_off += size;
        return req;
    }
    //read diff block data from snapshot
    DiffBlocks& diff_block = diff_blocks[vector_cursor];
    uint64_t diff_block_no = diff_block.diff_block_no(array_cursor);
    off_t    diff_block_off = diff_block_no * COW_BLOCK_SIZE;
    size_t   diff_block_size = COW_BLOCK_SIZE;
    StatusCode ret = SnapReader::instance().get_client()->ReadSnapshot(
        ctx->get_vol_id(),cur_snap,buffer,diff_block_size,diff_block_off);
    DR_ASSERT(ret == StatusCode::sOk);

    //construct JournalEntry
    JournalEntry entry;
    construct_journal_entry(entry,buffer,diff_block_off,diff_block_size);
    string entry_string;
    size_t size = entry.copy_entry(entry_string);

    // construct transfer request
    TransferRequest* req = new TransferRequest;
    // no enough space in cur journal file, use next
    if(cur_off + size > max_journal_size){
        ctx->set_j_counter(ctx->get_j_counter() + 1);
        cur_off = 0;
        // full journal file header
        journal_file_header_t header;
        header.magic = 0;
        header.version = 0;
        header.reserve = 0;
        entry_string.insert(0,(char*)(&header),sizeof(journal_file_header_t));
        size += sizeof(journal_file_header_t);

        construct_transfer_data_request(req,entry_string.c_str(),size,cur_off,
            ctx,++package_id);
        cur_off += size;
    }
    else{
        construct_transfer_data_request(req,entry_string.c_str(),size,cur_off,
            ctx,++package_id);
        cur_off += size;
    }

    //move cursors
    if(array_cursor + 1 >= diff_block.diff_block_no_size()){
        // check next diffblock
        vector_cursor++;
        array_cursor = 0;
        if(vector_cursor >= diff_blocks.size()){
            LOG_INFO << "all diff blocks were traversed in task["
                << this->get_id() << "]";
            all_data_sent = true;
        }
        else{
            if(diff_blocks[vector_cursor].diff_block_no_size()<=0){
                LOG_WARN << "no diff block was found in task["
                    << this->get_id() << "], cursor:" << vector_cursor;
                all_data_sent = true;
            }
        }
    }
    else{
        array_cursor++;
    }
    return req;
}

std::string& DiffSnapTask::get_pre_snap(){
    return pre_snap;
}
void DiffSnapTask::set_pre_snap(const std::string& _snap){
    pre_snap = _snap;
}

std::string& DiffSnapTask::get_cur_snap(){
    return cur_snap;
}
void DiffSnapTask::set_cur_snap(const std::string& _snap){
    cur_snap = _snap;
}


std::string& BaseSnapTask::get_base_snap(){
    return base_snap;
}
void BaseSnapTask::set_base_snap(const std::string& _snap){
    base_snap = _snap;
}

uint64_t BaseSnapTask::get_vol_size(){
    return vol_size;
}
void BaseSnapTask::set_vol_size(const uint64_t& _size){
    vol_size = _size;
}

// TODO: impliment
int BaseSnapTask::init(){
    package_id = 0;
    return 0;
}

bool BaseSnapTask::has_next_package(){
    return false;
}

TransferRequest* BaseSnapTask::get_next_package(){
    return 0;
}

