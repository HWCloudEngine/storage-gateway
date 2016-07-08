#include "server.hpp"
#include <boost/bind.hpp>

namespace Journal{

Server::Server(const std::string& address, const std::string& port)
	:io_service_(),
	acceptor_(io_service_),
	connection_manager_(),
	new_connection_()
{
	boost::asio::ip::tcp::resolver resolver(io_service_);
	boost::asio::ip::tcp::resolver::query query(address, port);
	boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(query);
	acceptor_.open(endpoint.protocol());
	acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
	acceptor_.bind(endpoint);
	acceptor_.listen();

	start_accept();
}

void Server::run()
{
	io_service_.run();
}

void Server::start_accept()
{
	new_connection_.reset(new Connection(io_service_,connection_manager_));
	acceptor_.async_accept(new_connection_->get_raw_socket(),
		boost::bind(&Server::handle_accept,this,boost::asio::placeholders::error));
}

void Server::handle_accept(const  boost::system::error_code & e)
{
	if(!e)
	{
		connection_manager_.start(new_connection_);
	}
	else
	{
		std::cerr << "Can not hanle this accept" << std::endl;
		//todo use log4cpp
	}
}

void Server::handle_stop()
{
	acceptor_.close();
	connection_manager_.stop_all();
}

}
