#include "pre_processor.hpp"

namespace Journal{

PreProcessor::PreProcessor(boost::asio::ip::tcp::socket& socket_,
                                     entry_queue& write_queue,
                                     entry_queue& entry_queue,
                                     std::mutex& write_mtx,
                                     std::condition_variable& write_cv)
    :write_queue_(write_queue),
    entry_queue_(entry_queue),
    buffer_pool_(NULL),
    worker_threads(),
    raw_socket_(socket_),
    write_mtx_(write_mtx),
    write_cv_(write_cv)
{
    //todo
}

PreProcessor::~PreProcessor()
{
    
}

void PreProcessor::work()
{
    //loop 
    //todo calculate crc and  merge request
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
