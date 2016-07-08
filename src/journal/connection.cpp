#include "connection.hpp"
#include <boost/bind.hpp>
#include "connection_manager.hpp"
#include "request_handler.hpp"

namespace Journal{

Connection::Connection(boost::asio::io_service& io_service,
			            ConnectionManager& manager,RequestHandler& handler)
			            :raw_socket_(io_service),
			            connection_manager_(manager),
			            request_handler_(handler)
{

}

void Connection::start()
{
	raw_socket_.async_read_some(boost::asio::buffer(buffer_),
		boost::bind(&Connection::handle_read, shared_from_this(),
		boost::asio::placeholders::error,
		boost::asio::placeholders::bytes_transferred));
	
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

void Connection::handle_read(const boost::system::error_code& e, std::size_t bytes_transferred)
{
	if(!e)
	{
		//todo use request_handeler
		std::cout << "test read" << std::endl;
	}
	else
	{
		//todo
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
