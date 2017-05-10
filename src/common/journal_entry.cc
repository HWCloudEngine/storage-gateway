/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   journal_entry.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: io and cmd format in journal file
*
*************************************************/
#include <sys/uio.h>
#include <assert.h>
#include "crc32.h"
#include "../log/log.h"
#include "../rpc/message.pb.h"
#include "journal_entry.h"
using huawei::proto::WriteMessage;
using huawei::proto::SnapshotMessage;

JournalEntry::JournalEntry() {
}

JournalEntry::JournalEntry(uint64_t& io_handle, uint64_t& seq, 
                    journal_event_type_t& type, std::shared_ptr<Message>&  message) {
    this->handle.push_back(io_handle);
    this->sequence = seq;
    this->type = type;
    this->length = 0;
    this->message = message;
    this->crc = 0;
    this->message_serialized_data.clear();
}

JournalEntry::JournalEntry(const JournalEntry& other) {
    this->handle = other.handle;
    this->sequence = other.sequence;
    this->type  = other.type;
    this->length = other.length;
    this->message = other.message;
    this->crc = other.crc;
    this->message_serialized_data.clear();
}

JournalEntry& JournalEntry::operator=(const JournalEntry& other) {
    this->handle = other.handle;
    this->sequence = other.sequence;
    this->type  = other.type;
    this->length = other.length;
    this->message = other.message;
    this->crc = other.crc;
    this->message_serialized_data = other.message_serialized_data;
    return *this;
}

JournalEntry::~JournalEntry() {
    /*todo free resource */
}

void JournalEntry::set_handle(uint64_t io_handle) {
    handle.push_back(io_handle);
}

vector<uint64_t> JournalEntry::get_handle()const {
    return handle;
}

void JournalEntry::set_sequence(uint64_t seq) {
    sequence = seq;
}

uint64_t JournalEntry::get_sequence()const {
    return sequence;
}

void JournalEntry::set_type(journal_event_type_t type) {
    this->type = type;
}

journal_event_type_t JournalEntry::get_type()const {
    return (journal_event_type_t)type;
}

void JournalEntry::set_length(uint32_t len) {
    this->length = len;
}

uint32_t JournalEntry::get_length()const {
    return length;
}

void JournalEntry::set_message(std::shared_ptr<Message> message) {
    this->message = message;
}

std::shared_ptr<Message> JournalEntry::get_message()const {
    return message;
}

void JournalEntry::set_crc(uint32_t crc) {
    this->crc = crc;
}

uint32_t JournalEntry::get_crc()const {
    return crc;
}

uint32_t JournalEntry::calculate_crc() {
    uint32_t initial = 0;
    assert(!message_serialized_data.empty());
    this->crc = crc32c(message_serialized_data.c_str(),
                       message_serialized_data.size(),
                       initial);
    return this->crc;
}

size_t JournalEntry::get_persit_size()const {
    return sizeof(type) + sizeof(length) + length + sizeof(crc);
}

ssize_t JournalEntry::persist(unique_ptr<AccessFile>* file, off_t off) {
    if (message_serialized_data.empty()) {
        serialize();
        calculate_crc();
    }
    size_t buf_len = sizeof(type) + sizeof(length) + length + sizeof(crc) ;
    char*  buf = (char*)malloc(buf_len);
    char*  write_buf = buf;    
    memcpy(write_buf, (char*)&type, sizeof(type));
    write_buf += sizeof(type);
    memcpy(write_buf, (char*)&length, sizeof(length));
    write_buf += sizeof(length);
    memcpy(write_buf, (char*)message_serialized_data.c_str(), length);
    write_buf += length;
    memcpy(write_buf, (char*)&crc, sizeof(crc));
    ssize_t ret = (*file)->write(buf, buf_len, off);
    assert(ret == buf_len);
    free(buf);
    //LOG_INFO << "persist type:" << type << " length:" << length 
    //         << " crc:" << crc  << " ok";
    return buf_len;
}

size_t JournalEntry::copy_entry(std::string& buffer) {
    buffer.append((char*)&type, sizeof(type));
    buffer.append((char*)&length, sizeof(length));
    buffer.append((char*)message_serialized_data.c_str(), length);
    buffer.append((char*)&crc, sizeof(crc));
    size_t size = sizeof(type) + sizeof(length) + length + sizeof(crc);
    return size;
}

ssize_t JournalEntry::parse(unique_ptr<AccessFile>* file, size_t fsize, off_t off) {
    off_t start = off;
    /*read type and length*/
    struct iovec iov[2];
    iov[0].iov_base = &type;
    iov[0].iov_len  = sizeof(type);
    iov[1].iov_base = &length;
    iov[1].iov_len  = sizeof(length);
    size_t read_ret = (*file)->readv(iov, 2, start);
    if (read_ret != (sizeof(type) + sizeof(length))) {
        LOG_ERROR << "parse type and length failed";
        return -1;
    }
    if (type != IO_WRITE && type != SNAPSHOT_CREATE && type != SNAPSHOT_DELETE &&
       type != SNAPSHOT_ROLLBACK) {
        LOG_ERROR << "parse type failed";
        return -1; 
    }
    if ((off + length) > fsize) {
        LOG_ERROR << "parse length over region failed";
        return -1;
    }
    start += read_ret;

    /*for store serialize data*/
    char* data = (char*)malloc(length);
    assert(data != nullptr);

    /*read data and crc*/
    struct iovec iov1[2];
    iov1[0].iov_base = data;
    iov1[0].iov_len  = length;
    iov1[1].iov_base = &crc;
    iov1[1].iov_len  = sizeof(crc);
    read_ret = (*file)->readv(iov1, 2, start);
    if (read_ret != (length + sizeof(crc))) {
        LOG_ERROR << "parse data and crc failed";
        return -1;
    }
    start += read_ret;

    /*message parse*/
    switch (type) {
    case IO_WRITE:
        message = std::make_shared<WriteMessage>(); 
        break;
    case SNAPSHOT_CREATE:
    case SNAPSHOT_DELETE:
    case SNAPSHOT_ROLLBACK:
        message = std::make_shared<SnapshotMessage>(); 
        break;
    default:
        assert(0);
        break;
    }

    message->ParseFromArray(data, length);

    /*check crc */
    uint32_t initial = 0;
    uint32_t crc_value = crc32c(data, length, initial);

    assert(crc_value == crc);

    free(data);

    //LOG_INFO << "parse type:" << type << " length:" << length 
    //         << " crc:" << crc << " crc_value:" << crc_value << " ok";
    return start;
}

bool JournalEntry::serialize() {
    message->SerializeToString(&message_serialized_data);
    this->length = message_serialized_data.size();
    return true;
}

bool JournalEntry::deserialize(const std::string& in) {
    message->ParseFromString(in);
    return true;
}

void JournalEntry::clear_serialized_data() {
    message_serialized_data.clear();
}

std::ostream& operator<<(std::ostream& cout, const JournalEntry& entry) {
    LOG_INFO << " seq:"  << entry.get_sequence()
             << " type:" << entry.get_type()
             << " len:"  << entry.get_length()
             << " crc:"  << entry.get_crc();
    return cout;
}
