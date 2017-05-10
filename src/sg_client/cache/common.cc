/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*  
*  File name:    backup_proxy.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  backup entry
*  
*************************************************/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <utility>
#include <sys/mman.h>
#include <algorithm>
#include <string.h>
#include "common.h"
#include "log/log.h"
#include "rpc/message.pb.h"

using google::protobuf::Message;
using huawei::proto::WriteMessage;

CEntry::CEntry(IoVersion seq, off_t bdev_off, size_t bdev_len,
               string jfile, off_t jfile_off) {
    io_seq  = seq;
    blk_off = bdev_off;
    blk_len = bdev_len;
    journal_file = jfile;
    journal_off = jfile_off;
    cache_type = IN_JOURANL;
    journal_entry = nullptr;
}

CEntry::CEntry(IoVersion seq, off_t bdev_off, size_t bdev_len,
               string jfile, off_t jfile_off,
               shared_ptr<JournalEntry> entry) {
    io_seq  = seq;
    blk_off = bdev_off;
    blk_len = bdev_len;
    journal_file = jfile;
    journal_off = jfile_off;
    cache_type = IN_MEM;
    journal_entry  = entry;
}

CEntry::CEntry(const CEntry& other) {
    io_seq  = other.io_seq;
    blk_off = other.blk_off;
    blk_len = other.blk_len;
    journal_file = other.journal_file;
    journal_off  = other.journal_off;
    cache_type = other.cache_type;
    journal_entry = other.journal_entry;
}

CEntry::CEntry(CEntry&& other) {
    *this = std::move(other);
}

CEntry& CEntry::operator=(const CEntry& other) {
    if (this != &other) {
        io_seq  = other.io_seq;
        blk_off = other.blk_off;
        blk_len = other.blk_len;
        journal_file = other.journal_file;
        journal_off = other.journal_off;
        cache_type = other.cache_type;
        journal_entry = other.journal_entry;
    } 
    return *this;
}

CEntry& CEntry::operator=(CEntry&& other) {
    if (this != &other) {
        io_seq  = other.io_seq;
        blk_off = other.blk_off;
        blk_len = other.blk_len;
        journal_file = other.journal_file;
        journal_off  = other.journal_off;
        cache_type = other.cache_type;
        journal_entry = other.journal_entry;
    }
    return *this;
}

size_t CEntry::get_mem_size()const {
    size_t size = 0;
    if (cache_type == IN_MEM) {
        if (IO_WRITE == journal_entry->get_type()) {
            shared_ptr<Message> message = journal_entry->get_message();
            shared_ptr<WriteMessage> write_message = dynamic_pointer_cast
                                                     <WriteMessage>(message);
            size += write_message->data().size();
        } else {
            /*other message*/
        }
    } else {
        ; 
    }
    return size;
}
