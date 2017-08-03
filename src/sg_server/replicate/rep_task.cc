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
#include "snapshot/snapshot_cli.h"
#include "rpc/message.pb.h"
#include "common/journal_entry.h"
#include "common/define.h"
#include "common/crc32.h"
#include "common/config_option.h"
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
// suppose that the serialized entry length was never longer than MAX_JOURNAL_ENTRY_LEN
#define PREFIX_DATA_LEN 128
#define MAX_JOURNAL_ENTRY_LEN (PREFIX_DATA_LEN + COW_BLOCK_SIZE)

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
        const string& vol_id,const int64_t& j_counter,
        const int64_t& sub_counter,
        const char* buffer, const uint64_t& offset,
        const size_t& size,const uint64_t& id){
    ReplicateDataReq data_msg;
    data_msg.set_vol_id(vol_id);
    data_msg.set_journal_counter(j_counter);
    data_msg.set_sub_counter(sub_counter);
    data_msg.set_offset(offset);
    data_msg.set_data(buffer,size);

    req->set_id(id);
    req->set_encode(EncodeType::NONE_EN);
    req->set_type(MessageType::REPLICATE_DATA);
    string temp;
    SG_ASSERT(true == data_msg.SerializeToString(&temp));
    req->set_data(temp.c_str(),temp.length());
    return 0;
}

int construct_transfer_start_request(TransferRequest* req,
                                const string& vol_id,
                                const int64_t& j_counter,
                                const int64_t& sub_counter,
                                const uint64_t& id){
    req->set_id(id);
    req->set_encode(EncodeType::NONE_EN);
    req->set_type(MessageType::REPLICATE_START);
    ReplicateStartReq start_msg;
    start_msg.set_vol_id(vol_id);
    start_msg.set_journal_counter(j_counter);
    start_msg.set_sub_counter(sub_counter);
    string temp;
    SG_ASSERT(true == start_msg.SerializeToString(&temp));
    req->set_data(temp.c_str(),temp.length());
    return 0;
}

int construct_transfer_end_request(TransferRequest* req,
            const string& vol_id,const int64_t& j_counter,
            const int64_t& sub_counter,
            const uint64_t& id, const bool& is_open){
    req->set_id(id);
    req->set_encode(EncodeType::NONE_EN);
    req->set_type(MessageType::REPLICATE_END);
    ReplicateEndReq end_msg;
    end_msg.set_vol_id(vol_id);
    end_msg.set_journal_counter(j_counter);
    end_msg.set_sub_counter(sub_counter);
    end_msg.set_is_open(is_open);
    string temp;
    SG_ASSERT(true == end_msg.SerializeToString(&temp));
    req->set_data(temp.c_str(),temp.length());
    return 0;
}

int JournalTask::init(){
    package_id = 0;
    SG_ASSERT(ctx != nullptr);
    is.open(path,std::fstream::in|std::ifstream::binary);
    if(!is.is_open()){
        LOG_ERROR << "open file:" << path << " of "
            << ctx->get_j_counter() << " error.";
        return -1;
    }
    cur_off = start_off;
    is.seekg(cur_off);
    SG_ASSERT(is.fail()==0 && is.bad()==0);

    LOG_DEBUG << "transfering journal " << std::hex
        << ctx->get_j_counter() << std::dec << " from "
        << start_off << " to " << ctx->get_end_off();

    buffer = (char*)malloc(JOURNAL_BLOCK_SIZE);
    if(buffer == nullptr){
        LOG_ERROR << "alloc buffer for journal task failed!";
        return -1;
    }
    return 0;
}

bool JournalTask::has_next_package(){
    if(get_status() == T_CANCELED || get_status() == T_ERROR)
        return false;
    return (!end);
}

// TODO: startReq
TransferRequest* JournalTask::get_next_package(){
    if(cur_off >= ctx->get_end_off()){
        TransferRequest* req = new TransferRequest;
        construct_transfer_end_request(req,ctx->get_peer_vol(),
                ctx->get_j_counter(),0,++package_id,ctx->get_is_open());
        end = true;
        LOG_DEBUG << "construct end req, " << ctx->get_peer_vol()
            << ":" << ctx->get_j_counter();
        return req;
    }

    size_t size = (ctx->get_end_off()-cur_off) > JOURNAL_BLOCK_SIZE ?
        JOURNAL_BLOCK_SIZE:(ctx->get_end_off() - cur_off);
    if(is.read(buffer,size)){

    }
    else if(is.eof()){
        LOG_WARN << "journal file[" << path << "] end at:" << cur_off + size;
        SG_ERROR_OCCURED();
    }
    else{
        LOG_ERROR << "read file[" << path << "] failed,required length["
            << size << "],offset[" << cur_off << "].";
        LOG_ERROR << "read fail:" << is.fail() << ",bad:" << is.bad();
        return nullptr;
    }

    TransferRequest* req = new TransferRequest;
    construct_transfer_data_request(req,ctx->get_peer_vol(),ctx->get_j_counter(),
            0,buffer,cur_off,size,++package_id);

    uint32_t crc = crc32c(buffer,size,0);
    LOG_DEBUG << "transfer file[" << path << "] from "<< cur_off 
        << ",len [" << size << "],crc[" << crc << "].";
    cur_off += size; // update offset
    return req;
}

int JournalTask::reset(){
    end = false;
    cur_off = start_off;
    // reopen journal file
    is.close();
    is.open(path,std::fstream::in | std::ifstream::binary);
    SG_ASSERT(true == is.is_open());
    is.seekg(cur_off);
    SG_ASSERT(is.fail()==0 && is.bad()==0);
    return 0;
}


int DiffSnapTask::init(){
    diff_blocks.clear();
    vector_cursor = 0;
    array_cursor = 0;
    all_data_sent = false;
    cur_off = 0;
    // sub counter should start at 1, or it will overlat the former journal
    sub_counter = 1;
    package_id = 0;
    // set journal max size
    max_journal_size = ctx->get_end_off();

    // init diff_blocks
    snap_cli = create_snapshot_rpc_client(ctx->get_vol_id());
    SG_ASSERT(snap_cli != nullptr);
    StatusCode ret = snap_cli->DiffSnapshot(
            ctx->get_vol_id(),pre_snap,cur_snap,diff_blocks);
    SG_ASSERT(ret == StatusCode::sOk);
    if(diff_blocks.empty() 
        || (diff_blocks.size() == 1 && diff_blocks[0].block_size() == 0)){
        LOG_INFO << "no data sync for diff snapshot,pre:" << cur_snap
            << ",pre:" << pre_snap;
        end = true;
    }

    buffer = (char*)malloc(COW_BLOCK_SIZE);
    if(buffer == nullptr){
        LOG_ERROR << "alloc buffer for diff block failed!";
        return -1;
    }
    return 0;
}

bool DiffSnapTask::has_next_package(){
    if(get_status() == T_CANCELED || get_status() == T_ERROR)
        return false;
    return (!end);
}

TransferRequest* DiffSnapTask::get_next_package(){
    if(!has_next_package()){
        return nullptr;
    }
    if(all_data_sent){ // all data sent
        TransferRequest* req = new TransferRequest;
        construct_transfer_end_request(req,ctx->get_peer_vol(),
                ctx->get_j_counter(),sub_counter,++package_id,ctx->get_is_open());
        end = true;
        LOG_DEBUG << "construct end req,peer volume: " << ctx->get_peer_vol()
            << ", cur_snap" << cur_snap;
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
        construct_transfer_data_request(req,ctx->get_peer_vol(),
            ctx->get_j_counter(),sub_counter,
            (char*)(&header),cur_off,size,++package_id);
        cur_off += size;
        return req;
    }

    DiffBlocks& diff_block = diff_blocks[vector_cursor];
    // construct transfer request
    TransferRequest* req = new TransferRequest;
    if(diff_block.block_size() > 0){// first diff_blocks may be empty
        // if no enough space in cur journal file, seal it
        if(cur_off + MAX_JOURNAL_ENTRY_LEN > max_journal_size){
            construct_transfer_end_request(req,ctx->get_peer_vol(),
                ctx->get_j_counter(),sub_counter,++package_id,ctx->get_is_open());
            LOG_DEBUG << "journal[" << sub_counter << "] is full"
                << ", cur_snap" << cur_snap;
            // next journal
            sub_counter++;
            SG_ASSERT(sub_counter < 0xffff);
            cur_off = 0;
            return req;
        }
        //read diff block data from snapshot
        uint64_t diff_block_no = diff_block.block(array_cursor).blk_no();
        off_t    diff_block_off = diff_block_no * COW_BLOCK_SIZE;
        size_t   diff_block_size = COW_BLOCK_SIZE;
        StatusCode ret = snap_cli->ReadSnapshot(
            ctx->get_vol_id(),cur_snap,buffer,diff_block_size,diff_block_off);
        SG_ASSERT(ret == StatusCode::sOk);

        //construct JournalEntry
        JournalEntry entry;
        construct_journal_entry(entry,buffer,diff_block_off,diff_block_size);
        string entry_string;
        size_t size = entry.copy_entry(entry_string);
        LOG_DEBUG << "get snap diff block no:" << diff_block_off
            << " ,vector cursor:" << vector_cursor
            << " ,array cursor:" << array_cursor
            << " ,entry len:" << entry.get_length()
            << " ,entry crc:" << entry.get_crc()
            << " ,entry_string len:" << entry_string.length();

        construct_transfer_data_request(req,ctx->get_peer_vol(),
            ctx->get_j_counter(),sub_counter,
            entry_string.c_str(),cur_off,size,++package_id);
        // debug
        uint32_t crc = crc32c(entry_string.c_str(),size,0);
        LOG_DEBUG << "transfer journal sub[" << sub_counter << "] from "<< cur_off 
            << ",len [" << size << "],crc[" << crc << "].";

        cur_off += size;
    }
    //move cursors
    if(array_cursor + 1 >= diff_block.block_size()){
        // check next diffblock
        vector_cursor++;
        array_cursor = 0;
        if(vector_cursor >= diff_blocks.size()){
            LOG_INFO << "all diff blocks were traversed in task["
                << this->get_id() << "]";
            all_data_sent = true;
        }
        else{
            if(diff_blocks[vector_cursor].block_size()<=0){
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

int DiffSnapTask::reset(){
    // TODO: resume at breakpoint
    end = false;
    vector_cursor = 0;
    array_cursor = 0;
    all_data_sent = false;
    cur_off = 0;
    sub_counter = 1;
    return 0;
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

int BaseSnapTask::init(){
    cur_off = 0;
    sub_counter = 1;
    read_off = 0;
    end = false;
    buffer = (char*)malloc(COW_BLOCK_SIZE);
    if(buffer == nullptr){
        LOG_ERROR << "alloc buffer for diff block failed!";
        return -1;
    }
    max_journal_size = ctx->get_end_off();
    snap_cli = create_snapshot_rpc_client(ctx->get_vol_id());
    SG_ASSERT(snap_cli != nullptr);
    return 0;
}

bool BaseSnapTask::has_next_package(){
    if(get_status() == T_CANCELED || get_status() == T_ERROR)
        return false;
    return (!end);
}

TransferRequest* BaseSnapTask::get_next_package(){
    if(!has_next_package())
        return nullptr;

    if(read_off >= vol_size){
        // construct end cmd
        TransferRequest* req = new TransferRequest;
        construct_transfer_end_request(req,ctx->get_peer_vol(),
                ctx->get_j_counter(),sub_counter,++package_id,ctx->get_is_open());
        end = true;
        LOG_INFO << "transfer base snap end:" << base_snap;
        return req;
    }

    if(cur_off == 0){
        // write journal file header:
        TransferRequest* req = new TransferRequest;
        journal_file_header_t header;
        header.magic = 0;
        header.version = 0;
        header.reserve = 0;
        size_t size = sizeof(journal_file_header_t);
        construct_transfer_data_request(req,ctx->get_peer_vol(),
            ctx->get_j_counter(),sub_counter,
            (char*)(&header),cur_off,size,++package_id);
        cur_off += size;
        return req;
    }
    // if no enough space in cur journal file, seal it
    if(cur_off + MAX_JOURNAL_ENTRY_LEN > max_journal_size){
        TransferRequest* req = new TransferRequest;
        construct_transfer_end_request(req,ctx->get_peer_vol(),
            ctx->get_j_counter(),sub_counter,++package_id,ctx->get_is_open());
        LOG_DEBUG << "journal[" << sub_counter << "] is full"
            << ", base_snap" << base_snap;
        // next journal
        sub_counter++;
        SG_ASSERT(sub_counter < 0xffff);
        cur_off = 0;
        return req;
    }
    // read snapshot
    uint64_t len = vol_size - read_off > COW_BLOCK_SIZE ?
        COW_BLOCK_SIZE:(vol_size - read_off);
    StatusCode ret = snap_cli->ReadSnapshot(
        ctx->get_vol_id(),base_snap,buffer,len,read_off);
    SG_ASSERT(ret == StatusCode::sOk);

    //construct JournalEntry
    JournalEntry entry;
    construct_journal_entry(entry,buffer,read_off,len);
    string entry_string;
    size_t size = entry.copy_entry(entry_string);
    LOG_DEBUG << "get snap block offset:" << read_off
        << " ,entry len:" << entry.get_length()
        << " ,entry crc:" << entry.get_crc()
        << " ,entry_string len:" << entry_string.length();

    // construct transfer request
    TransferRequest* req = new TransferRequest;
    construct_transfer_data_request(req,ctx->get_peer_vol(),
        ctx->get_j_counter(),sub_counter,
        entry_string.c_str(),cur_off,size,++package_id);

    uint32_t crc = crc32c(entry_string.c_str(),size,0);
    LOG_DEBUG << "transfer journal sub[" << sub_counter << "] from "<< cur_off 
        << ",len [" << size << "],crc[" << crc << "].";

    cur_off += size;

    // update read offset
    read_off += size;
    return req;
}

int BaseSnapTask::reset(){
    cur_off = 0;
    sub_counter = 1;
    read_off = 0;
    end = false;
    return 0;
}

