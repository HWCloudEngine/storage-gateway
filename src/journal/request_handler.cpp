#include "request_handler.hpp"

namespace Journal{

RequestHandler::RequestHandler(boost::asio::ip::tcp::socket& socket_,entry_queue& queue_)
    :entry_queue_(queue_),
    buffer_pool_(NULL),
    worker_threads(),
    raw_socket_(socket_)
{
	//todo
}

RequestHandler::~RequestHandler()
{
    //todo
}

bool RequestHandler::handle_request(char* buffer,uint32_t size)
{
    if (buffer == NULL)
    {
        //LOG
        return false;
    }
    entry_ptr entry_ptr_ = NULL;
    try
    {
        entry_ptr_= new ReplayEntry(buffer,size);
    }
    catch(const std::bad_alloc & e)
    {
        //LOG
        return false;
    }
    if(entry_ptr_ == NULL)
    {
        //LOG
        return false;
    }
    if(!entry_queue_.push(entry_ptr_))
    {
        //todo LOG
        delete entry_ptr_;
        return false;
    }
    //todo
    return true;
}

void RequestHandler::work()
{
    //loop 
    //todo
}

bool RequestHandler::init(nedalloc::nedpool * buffer_pool,int thread_num)
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
        worker_threads.create_thread(boost::bind(&RequestHandler::work,this));
    }
    worker_threads.join_all();
    return true;
}
bool RequestHandler::deinit()
{
    //todo
}

}
