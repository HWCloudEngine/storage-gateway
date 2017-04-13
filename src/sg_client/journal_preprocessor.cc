#include "journal_preprocessor.h"
#include "../common/crc32.h"
#include "../common/xxhash.h"
#include "../common/utils.h"
#include "../log/log.h"

namespace Journal{

JournalPreProcessor::JournalPreProcessor(BlockingQueue<shared_ptr<JournalEntry>>& entry_queue,
                           BlockingQueue<shared_ptr<JournalEntry>>& write_queue)
    :entry_queue_(entry_queue),
     write_queue_(write_queue),
     worker_threads()
{
    LOG_INFO << "JournalProcessor create";
}

JournalPreProcessor::~JournalPreProcessor()
{
    LOG_INFO << "JournalProcessor destroy ";
}

void JournalPreProcessor::work()
{
    BlockingQueue<shared_ptr<JournalEntry>>::position pos;

    while(running_flag)
    {
        shared_ptr<JournalEntry> entry;
        bool ret = entry_queue_.pop(entry, write_queue_, pos);    
        if(!ret){
            return; 
        }
        /*message serialize*/
        entry->serialize();

        /*calculate crc*/ 
        entry->calculate_crc();

        write_queue_.push(entry, pos);
    }
}

bool JournalPreProcessor::init(const Configure& conf)
{
    conf_ = conf;
    running_flag = true;

    for (int i=0;i < conf_.journal_process_thread_num; i++)
    {
        worker_threads.create_thread(boost::bind(&JournalPreProcessor::work,this));
    }
    return true;
}

bool JournalPreProcessor::deinit()
{
    running_flag = false;
    entry_queue_.stop();
    worker_threads.join_all();
    return true;
}

}
