#ifndef JOURNAL_SERVER_HPP
#define JOURNAL_SERVER_HPP

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <string>
#include "connection.hpp"
#include "connection_manager.hpp"
#include "request_handler.hpp"

namespace Journal{

class Server
	:private boost::noncopyable
{
public:
	explicit Server(const std::string& address, const std::string& port);
	void run();

private:
	void start_accept();
	void handle_accept(const boost::system::error_code& e);
	void handle_stop();

	boost::asio::io_service io_service_;
	boost::asio::ip::tcp::acceptor acceptor_;

	ConnectionManager connection_manager_;
	connection_ptr new_connection_;
	RequestHandler request_handler_;
};
}

#endif		