/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_reader.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  handle read io
*
*************************************************/
#ifndef SRC_SG_CLIENT_JOURNAL_READER_H_
#define SRC_SG_CLIENT_JOURNAL_READER_H_
#include <memory>
#include <thread>
#include "common/blocking_queue.h"
#include "message.h"
#include "cache/cache_proxy.h"

class JournalReader {
 public:
    explicit JournalReader(BlockingQueue<io_request_t>& read_queue,
                           BlockingQueue<io_reply_t*>& reply_queue);
    virtual ~JournalReader();
    JournalReader(const JournalReader& r) = delete;
    JournalReader& operator=(const JournalReader& r) = delete;

    bool init(shared_ptr<CacheProxy> cacheproxy);
    bool deinit();
    void work();

 private:
    /*in queue*/
    BlockingQueue<io_request_t>& m_read_queue;
    /*out queue*/
    BlockingQueue<io_reply_t*>& m_reply_queue;
    shared_ptr<CacheProxy> m_cacheproxy;
    bool m_run;
    shared_ptr<thread> m_thread;
};

#endif  // SRC_SG_CLIENT_JOURNAL_READER_H_


