/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_reader.cc 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  handle read io
*
*************************************************/
#include "log/log.h"
#include "perf_counter.h"
#include "journal_reader.h"

JournalReader::JournalReader(BlockingQueue<io_request_t>& read_queue,
                             BlockingQueue<io_reply_t*>& reply_queue)
    :m_read_queue(read_queue), m_reply_queue(reply_queue), m_run(false) {
    LOG_INFO << "io reader work thread create";
    m_run = false;
    LOG_INFO << "io reader work thread create ok";
}

JournalReader::~JournalReader() {
    LOG_INFO << "io reader work thread destory";
    LOG_INFO << "io reader work thread destory ok";
}


bool JournalReader::init(shared_ptr<CacheProxy> cacheproxy) {
    m_cacheproxy = cacheproxy;
    m_run = true;
    m_thread.reset(new thread(std::bind(&JournalReader::work, this)));
    return true;
}

bool JournalReader::deinit() {
    m_run = false;
    m_read_queue.stop();
    m_thread->join();
    return true;
}

void JournalReader::work() {
    while (m_run) {
        /*fetch io read request*/
        io_request_t ioreq;
        bool rval = m_read_queue.pop(ioreq);
        if (!rval) {
            break;
        }

        do_perf(READ_BEGIN, ioreq.seq);

        int iorsp_len = sizeof(io_request_t) + ioreq.len;
        io_reply_t* iorsp = (io_reply_t*)new char[iorsp_len];
        iorsp->magic = ioreq.magic;
        iorsp->seq = ioreq.seq;
        iorsp->handle = ioreq.handle;
        char* buf = reinterpret_cast<char*>(iorsp) + sizeof(io_reply_t);
        LOG_DEBUG << "read" << " seq:" << ioreq.seq
                  << " off:" << ioreq.offset << " len:" << ioreq.len;
        /*read*/
        int ret = m_cacheproxy->read(ioreq.offset, ioreq.len, buf);
        iorsp->error = 0;
        iorsp->len = ioreq.len;
        /*send reply*/
        if (!m_reply_queue.push(iorsp)) {
            delete [] iorsp;
            return;
        }

        do_perf(READ_END, ioreq.seq);
    }
}
