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
using google::protobuf::Message;
using huawei::proto::WriteMessage;
using huawei::proto::DiskPos;

int construct_journal_entry(JournalEntry& entry, 
                    char* data, const uint64_t& off, const size_t& len){
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

int JournalTask::init(){
    is.open(path,std::ifstream::binary);
    if(!is.is_open()){
        LOG_ERROR << "open file:" << path << " of "
            << j_counter << " error.";
        return -1;
    }
    cur_off = start_off;
    is.seekg(cur_off);
    DR_ASSERT(is.fail()==0 && is.bad()==0);
    LOG_DEBUG << "transfering journal " << j_counter << " from "
        << start_off << " to " << end_off;
    return 0;
}

bool JournalTask::has_next_block(){
    if(cur_off < end_off)
        return true;
    else
        return false;
}

int JournalTask::get_next_block(int64_t& counter,char* data,
                            uint64_t& off,const size_t& len){
    counter = j_counter;
    off = cur_off;
    int size = (end_off-cur_off) > len ? len:(end_off-cur_off);
    if(is.read(data,size)){
        cur_off += size;
        return size;
    }
    else{
        if(is.eof() && is.gcount()>0){
            cur_off += is.gcount();
            // TODO: remove this line if file created with padding filled
            set_end_off(cur_off);
            LOG_DEBUG << "journal file[" << path << "] end at:" << cur_off;
            return is.gcount();
        }
        LOG_ERROR << "read file[" << path << "] failed,required length["
            << size << "].";
        return -1;
    }
}

// TODO:
int DiffSnapTask::init(){
    diff_blocks.clear();
    vector_cursor = 0;
    array_cursor = 0;
    traverse_all = false;
    cur_off = 0;
    max_journal_size = 1L << 31; // TODO
    // set block size
    JournalEntry entry;
    size_t size = 0;
    size += sizeof(entry.get_crc());
    size += sizeof(entry.get_type());
    size += sizeof(entry.get_length());
    size += COW_BLOCK_SIZE; // TODO:suppose that serialized protobuf message is shorter
    this->set_block_size(size);
    // init diff_blocks
    StatusCode ret = SnapReader::instance().get_client()->DiffSnapshot(
            vol_id,pre_snap,cur_snap,diff_blocks);
    DR_ASSERT(ret == StatusCode::sOk);
    if(diff_blocks.empty()){
        LOG_INFO << "there was no diff block in task[" << this->get_id()
                << "]";
        traverse_all = true;
        return 0;
    }
    else{
        if(diff_blocks[0].diff_block_no_size()<=0){
            LOG_INFO << "there was no diff block in task[" << this->get_id()
                << "]";
            traverse_all = true;
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

bool DiffSnapTask::has_next_block(){
    return !traverse_all;
}

int DiffSnapTask::get_next_block(int64_t& counter,char* data,
                                uint64_t& off,const size_t& len){
    if(!has_next_block()){
        return -1;
    }

    //read diff block data from snapshot
    DiffBlocks& diff_block = diff_blocks[vector_cursor];
    uint64_t diff_block_no = diff_block.diff_block_no(array_cursor);
    off_t    diff_block_off = diff_block_no * COW_BLOCK_SIZE;
    size_t   diff_block_size = COW_BLOCK_SIZE;
    StatusCode ret = SnapReader::instance().get_client()->ReadSnapshot(
                    vol_id,cur_snap,buffer,diff_block_size,diff_block_off);
    DR_ASSERT(ret == StatusCode::sOk);

    //construct JournalEntry
    JournalEntry entry;
    construct_journal_entry(entry,buffer,diff_block_off,diff_block_size);
    size_t size = entry.copy_entry(data,len);
    if(cur_off + size > max_journal_size){
        DR_ERROR_OCCURED(); // TODO
    }
    else{
        counter = j_counter;
        off = cur_off;
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
            traverse_all = true;
        }
        else{
            if(diff_blocks[vector_cursor].diff_block_no_size()<=0){
                LOG_WARN << "no diff block was found in task["
                    << this->get_id() << "], cursor:" << vector_cursor;
                traverse_all = true;
            }
        }
    }
    else{
        array_cursor++;
    }
    return size;
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

int BaseSnapTask::init(){
    return 0;
}
// TODO:
bool BaseSnapTask::has_next_block(){
    return false;
}

int BaseSnapTask::get_next_block(int64_t& counter,char* data,
                                uint64_t& off,const size_t& len){
    return 0;
}

