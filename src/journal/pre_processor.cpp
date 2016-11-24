#include "pre_processor.hpp"
#include "../common/crc32.h"
#include "../common/xxhash.h"
#include "../common/utils.h"
#include "../log/log.h"

namespace Journal{

PreProcessor::PreProcessor(PRQueue<shared_ptr<JournalEntry>>& entry_queue,
                           PRQueue<shared_ptr<JournalEntry>>& write_queue)
    :entry_queue_(entry_queue),
     write_queue_(write_queue),
     worker_threads()
{
}

PreProcessor::~PreProcessor()
{
}

void PreProcessor::work()
{
    PRQueue<shared_ptr<JournalEntry>>::position pos;

    while(running_flag)
    {
        shared_ptr<JournalEntry> entry;
        entry_queue_.pop(entry, write_queue_, pos);    

        /*message serialize*/
        entry->serialize();

        /*calculate crc*/ 
        entry->calculate_crc();

        write_queue_.push(entry, pos);
    }
}

bool PreProcessor::init(std::shared_ptr<ConfigParser> conf)
{
    running_flag = true;

    config.checksum_type = (checksum_type_t)conf->get_default("pre_processor.checksum_type",0);
    config.thread_num = conf->get_default("pre_processor.thread_num",1);
    if(config.thread_num <= 0)
    {
        return false;
    }
    config.thread_num = 3;
    for (int i=0;i < config.thread_num;i++)
    {
        worker_threads.create_thread(boost::bind(&PreProcessor::work,this));
    }
    return true;
}

bool PreProcessor::deinit()
{
    running_flag = false;
    worker_threads.join_all();
    return true;
}

}
