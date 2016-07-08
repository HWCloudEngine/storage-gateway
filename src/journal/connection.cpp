#include "connection.hpp"
#include <boost/bind.hpp>
#include "connection_manager.hpp"
#include "request_handler.hpp"


#if !defined(USE_NEDMALLOC_DLL)
#include "nedmalloc.c"
#elif defined(WIN32)
#define WIN32_LEAN_AND_MEAN 1
#include <windows.h>
#endif

#ifdef _MSC_VER
#define MEMALIGNED(v) __declspec(align(v))
#elif defined(__GNUC__)
#define MEMALIGNED(v) __attribute__ ((aligned(v)))
#else
#define MEMALIGNED(v)
#endif


namespace Journal{

Connection::Connection(boost::asio::io_service& io_service,
			            ConnectionManager& manager)
			            :raw_socket_(io_service),
			            connection_manager_(manager),
			            request_handler_(raw_socket_)
{
    //todo read thread_num from config file
    int thread_num = 1;
	buffer_pool = nedalloc::nedcreatepool(BUFFER_POOL_SIZE,thread_num+1);
	if (buffer_pool == NULL)
	{
		//todo use LOG
		std::cout << "create buffer pool failed" << std::endl;
	}
    bool ret = request_handler_.init(buffer_pool,thread_num);
    if(!ret)
    {
        std::cout << "init request handler failed" << std::endl;
    }

}

Connection::~Connection()
{
    request_handler_.deinit();
    
	if (buffer_pool != NULL)
	{
		nedalloc::neddestroypool(buffer_pool);
		buffer_pool = NULL;
	}
}

void Connection::start()
{
    boost::asio::async_read(raw_socket_,
        boost::asio::buffer(header_buffer_, sizeof(struct Request)),
        boost::bind(&Connection::handle_read_header, shared_from_this(),
                     boost::asio::placeholders::error));
	
}

void Connection::stop()
{
	boost::system::error_code ignored_ec;
	raw_socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both,ignored_ec);
}

raw_socket& Connection::get_raw_socket()
{
	return raw_socket_;
}

void Connection::handle_read_header(const boost::system::error_code& e)
{
	Request* header_ptr = reinterpret_cast<Request *>(header_buffer_.data());
	if(!e && header_ptr->magic == MESSAGE_MAGIC )
	{
		if (header_ptr->len > 0)
		{
			uint32_t header_size = sizeof(struct Request);
			uint32_t buffer_size = header_ptr->len + sizeof(struct Request);
			// buffer_ptr will be free when we finish journal write,use nedpfree
			char* buffer_ptr = reinterpret_cast<char *>(nedalloc::nedpmalloc(buffer_pool,buffer_size));
			if (buffer_ptr!=NULL)
			{
				boost::asio::async_read(raw_socket_,
						boost::asio::buffer(buffer_ptr+header_size, buffer_size),
			            boost::bind(&Connection::handle_read_body, shared_from_this(),buffer_ptr,buffer_size,
			            boost::asio::placeholders::error));
			}
			else
			{
				//todo block or remalloc
				;
			}

		}
		else
		{
			//todo just a header?
			;
		}
	}
	else
	{
		//todo bad request
		;
	}

}

void Connection::handle_read_body(char* buffer_ptr,uint32_t buffer_size,const boost::system::error_code& e)
{
	if(!e)
	{
		*(reinterpret_cast<Request *>(buffer_ptr)) = *(reinterpret_cast<Request *>(header_buffer_.data()));
        bool ret = request_handler_.handle_request(buffer_ptr,buffer_size);
        if (!ret)
        {
            std::cout << "handle request failed" << std::endl;
            //todo reply client error code
        }
        boost::asio::async_read(raw_socket_,
            boost::asio::buffer(header_buffer_, sizeof(struct Request)),
            boost::bind(&Connection::handle_read_header, shared_from_this(),
            boost::asio::placeholders::error));
	}
	else
	{
		//todo bad request
		;
	}

}	

void Connection::handle_write(const boost::system::error_code& e, std::size_t bytes_transferred)
{
	if(!e)
	{
		//todo
		std::cout << "test write" << std::endl;
	}
	else
	{
		//todo
		;
	}
}
}
