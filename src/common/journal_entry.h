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
#ifndef SRC_COMMON_JOURNAL_ENTRY_H_
#define SRC_COMMON_JOURNAL_ENTRY_H_
#include "stdio.h"
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <google/protobuf/message.h>
#include "env_posix.h"
#include "define.h"

using google::protobuf::Message;

class JournalEntry {
 public:
    JournalEntry();
    JournalEntry(uint64_t& io_handle, uint64_t& seq, journal_event_type_t& type,
                 std::shared_ptr<Message>&  message);
    JournalEntry(const JournalEntry& other);
    JournalEntry& operator=(const JournalEntry& other);

    JournalEntry(const JournalEntry&& other) = delete;
    JournalEntry& operator=(const JournalEntry&& other) = delete;

    ~JournalEntry();

    void set_handle(uint64_t io_handle);
    vector<uint64_t> get_handle()const;
    void set_sequence(uint64_t seq);
    uint64_t get_sequence()const;
    void set_type(journal_event_type_t type);
    journal_event_type_t get_type() const;
    void set_length(uint32_t len);
    uint32_t get_length()const;
    void set_message(std::shared_ptr<Message> message);
    std::shared_ptr<Message> get_message()const;
    void set_crc(uint32_t crc);
    uint32_t get_crc()const;

    /*over protobuf message*/
    bool serialize();
    bool deserialize(const std::string& in);

    /*calculate crc on message serialize data*/
    uint32_t calculate_crc();

    /*persist the JournalEntry into journal file*/
    ssize_t persist(unique_ptr<AccessFile>* file, off_t off);
    /*copy serialized entry to data buffer*/
    size_t copy_entry(std::string& buffer);

    /*read from journal file and parse into JournalEnry*/
    ssize_t parse(unique_ptr<AccessFile>* file, size_t fsize, off_t off);

    /*persit data size*/
    size_t get_persit_size()const;

    /*after persist JournalEntry, free temporary message serialized data*/
    void clear_serialized_data();

    /*debug output*/
    friend std::ostream& operator<<(std::ostream& cout, const JournalEntry& entry);

 private:
    /*internal helper members*/
    vector<uint64_t> handle;   /*hook message response(io merge scanirios)*/
    uint64_t         sequence; /*internal sequence(parallel crc calculate)*/
    /*in memory */
    uint32_t type;    /*journal event type*/
    uint32_t length;  /*message serialize size*/
    std::shared_ptr<Message> message; /*protobuf base class*/
    uint32_t crc;     /*crc(message serialize data crc)*/
    /*temporary store message seriralize data*/
    std::string message_serialized_data;

    /*journal layout:
     * type(4Bytes) + length(4Bytes) + data(protoc serialize data) + crc(4Bytes)
     */
};

#endif  // SRC_COMMON_JOURNAL_ENTRY_H_
