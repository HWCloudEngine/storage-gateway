#include "pre_processor.hpp"
#include "../common/crc32.h"
#include "../common/xxhash.h"
#include "../common/utils.h"

namespace Journal{

PreProcessor::PreProcessor(entry_queue& write_queue,
                                     entry_queue& entry_queue,
                                     std::condition_variable& recieve_cv,
                                     std::condition_variable& write_cv)
    :write_queue_(write_queue),
    entry_queue_(entry_queue),
    buffer_pool_(NULL),
    worker_threads(),
    recieve_cv_(recieve_cv),
    write_cv_(write_cv)
{
    
}

PreProcessor::~PreProcessor()
{
    
}

bool PreProcessor::cal_checksum(ReplayEntry * entry,bool sse_flag,checksum_type_t checksum_type)
{
    uint32_t crc_32 = 0;
    uint64_t crc_64 = 0;
    if (NULL == entry)
        return false;
    log_header_t* header = entry->header();
    checksum_t& checksum = header->checksum;
    if(sse_flag && checksum_type == CRC_32)
    {
        crc_32 =  crc32c(entry->body(),entry->body_length(),crc_32);
        checksum.crc_32 = crc_32;
        return true;
    }
    if(checksum_type == CRC_32)
    {
        crc_32 = XXH32(entry->body(),entry->body_length(),crc_32);
        checksum.crc_32 = crc_32;
        return true;
    }
    if(checksum_type == CRC_64)
    {
        crc_64 = XXH64(entry->body(),entry->body_length(),crc_64);
        checksum.crc_64 = crc_64;
        return true;
    }
    //todo md5
    return true;
}

void PreProcessor::work()
{
    //todo read from config.ini
    checksum_type_t checksum_type = CRC_32;
    bool sse_flag = is_support_sse4_2();
    ReplayEntry* entry = NULL;
    while(true)
    {
        std::unique_lock<std::mutex> lk(mtx_);
        while(entry_queue_.empty())
        {
            recieve_cv_.wait(lk);
        }
        if(!entry_queue_.pop(entry))
        {
            LOG_ERROR << "entry_queue_ pop failed";
            continue;
        }
        if (NULL == entry)
        {
            LOG_ERROR << "entry ptr NULL";
            continue;
        }
        if(!cal_checksum(entry,sse_flag,checksum_type))
        {
            LOG_ERROR << "cal_crc failed";
        }
        if(!write_queue_.push(entry))
        {
            LOG_ERROR << "write_queue_ push failed";
            //todo handle error
        }
        write_cv_.notify_one();
    }
}

bool PreProcessor::init(nedalloc::nedpool * buffer_pool,int thread_num)
{
    if (buffer_pool == NULL)
    {
        return false;
    }
    buffer_pool_ = buffer_pool;
    if(thread_num <= 0)
    {
        return false;
    }
    for (int i=0;i < thread_num;i++)
    {
        worker_threads.create_thread(boost::bind(&PreProcessor::work,this));
    }
    return true;
}
bool PreProcessor::deinit()
{
    worker_threads.interrupt_all();
    worker_threads.join_all();
    return true;
}

}
