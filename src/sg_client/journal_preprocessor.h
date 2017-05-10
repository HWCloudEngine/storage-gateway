/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
*
*  File name:   journal_preprocessor.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: merge write io and calc crc
*
*************************************************/
#ifndef SRC_SG_CLIENT_JOURNAL_PREPROCESSOR_H_
#define SRC_SG_CLIENT_JOURNAL_PREPROCESSOR_H_

#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include "common/blocking_queue.h"
#include "common/journal_entry.h"
#include "message.h"

namespace Journal {

class JournalPreProcessor : private boost::noncopyable {
 public:
    explicit JournalPreProcessor(BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                                 BlockingQueue<shared_ptr<JournalEntry>>& write_queue);
    virtual ~JournalPreProcessor();

    void work();
    bool init();
    bool deinit();

 private:
    /*input queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& entry_queue_;
    /*output queue*/
    BlockingQueue<shared_ptr<JournalEntry>>& write_queue_;
    /*crc and io merge thread group*/
    bool running_flag;
    boost::thread_group worker_threads;
};

}  // namespace Journal

#endif  // SRC_SG_CLIENT_JOURNAL_PREPROCESSOR_H_
