#ifndef JOURNAL_REQUEST_HANDLER_HPP
#define JOURNAL_REQUEST_HANDLER_HPP

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>  
#include <boost/thread/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include "message.hpp"
#include "journal_writer.hpp"

namespace Journal{

class RequestHandler
	:private boost::noncopyable
{
public:
	explicit RequestHandler(boost::asio::ip::tcp::socket& socket_);
    virtual ~RequestHandler();
	bool handle_request(char* buffer,uint32_t size);
    void work();
    bool init(nedalloc::nedpool* buffer_pool,int thread_num);
    bool deinit();

private:
	JournalWriter journal_writer;
    boost::lockfree::queue<entry_ptr> entry_queue_;
    boost::thread_group worker_threads;
    nedalloc::nedpool* buffer_pool_;
    boost::asio::ip::tcp::socket& raw_socket_;
};

}

#endif