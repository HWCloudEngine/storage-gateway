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

namespace Journal {
JournalReader::JournalReader(BlockingQueue<struct IOHookRequest>& read_queue,
                             BlockingQueue<struct IOHookReply*>&  reply_queue)
    :m_read_queue(read_queue), m_reply_queue(reply_queue), m_run(false) {
    LOG_INFO << "IOReader work thread create";
}

JournalReader::~JournalReader() {
    LOG_INFO << "IOReader work thread destory";
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
        struct IOHookRequest ioreq;
        bool rval = m_read_queue.pop(ioreq);
        if (!rval) {
            break;
        }

        IoProbe* probe = g_perf.retrieve(ioreq.seq);
        if (probe) {
            probe->read_begin_ts = Env::instance()->now_micros();
        }

        int iorsp_len = sizeof(struct IOHookReply) + ioreq.len;
        struct IOHookReply* iorsp = (struct IOHookReply*)new char[iorsp_len];
        iorsp->magic = ioreq.magic;
        iorsp->seq = ioreq.seq;
        iorsp->handle = ioreq.handle;
        char* buf = reinterpret_cast<char*>(iorsp) + sizeof(struct IOHookReply);
        LOG_DEBUG << "read" << " hdl:" << ioreq.handle
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
        if (probe) {
            probe->read_end_ts = Env::instance()->now_micros();
        }
    }
}

}  // namespace Journal
