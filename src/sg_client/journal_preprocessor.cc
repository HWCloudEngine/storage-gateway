/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_preprocessor.c 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: merge write io and calc crc
*
*************************************************/
#include "common/crc32.h"
#include "common/xxhash.h"
#include "common/utils.h"
#include "common/config_option.h"
#include "log/log.h"
#include "journal_preprocessor.h"

namespace Journal {

JournalPreProcessor::JournalPreProcessor(BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                           BlockingQueue<shared_ptr<JournalEntry>>& write_queue)
    :entry_queue_(entry_queue),
     write_queue_(write_queue),
     worker_threads() {
    LOG_INFO << "JournalProcessor create";
}

JournalPreProcessor::~JournalPreProcessor() {
    LOG_INFO << "JournalProcessor destroy";
}

void JournalPreProcessor::work() {
    BlockingQueue<shared_ptr<JournalEntry>>::position pos;

    while (running_flag) {
        shared_ptr<JournalEntry> entry;
        bool ret = entry_queue_.pop(entry, write_queue_, pos);
        if (!ret) {
            return;
        }
        /*message serialize*/
        entry->serialize();

        /*calculate crc*/
        entry->calculate_crc();
        LOG_INFO << "xxx processor work";

        write_queue_.push(entry, pos);
    }
}

bool JournalPreProcessor::init() {
    running_flag = true;
    for (int i=0; i < g_option.journal_process_thread_num; i++) {
        worker_threads.create_thread(boost::bind(&JournalPreProcessor::work, this));
    }
    return true;
}

bool JournalPreProcessor::deinit() {
    running_flag = false;
    entry_queue_.stop();
    worker_threads.join_all();
    return true;
}
}  // namespace Journal
