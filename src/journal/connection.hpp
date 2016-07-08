#ifndef JOURNAL_CONNECTION_HPP
#define JOURNAL_CONNECTION_HPP

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/array.hpp>

#include "request_handler.hpp"
#include "message.hpp"
#include "nedmalloc.h"

#define HEADER_SIZE 128
#define BUFFER_POOL_SIZE 1024*1024*32
#define MESSAGE_MAGIC (0xAA)

namespace Journal{

typedef boost::asio::ip::tcp::socket raw_socket;
class ConnectionManager;

class Connection
	:public boost::enable_shared_from_this<Connection>,
	 private boost::noncopyable
{
public:
	explicit Connection(boost::asio::io_service& io_service,
			            ConnectionManager& manager);
	virtual ~Connection();
	void start();
    void stop();

	raw_socket& get_raw_socket();

private:
	void handle_read_header(const boost::system::error_code& error);
	void handle_read_body(char* buffer_ptr,uint32_t buffer_size,const boost::system::error_code& e);
	void handle_write(const boost::system::error_code& e, std::size_t bytes_transferred);

	raw_socket raw_socket_;
	ConnectionManager& connection_manager_;
	RequestHandler request_handler_;
	boost::array<char, HEADER_SIZE> header_buffer_;
	nedalloc::nedpool * buffer_pool;

};

typedef boost::shared_ptr<Connection> connection_ptr;
}

#endif
