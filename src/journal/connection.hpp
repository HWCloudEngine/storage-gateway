#ifndef JOURNAL_CONNECTION_HPP
#define JOURNAL_CONNECTION_HPP

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/array.hpp>

#include "message.hpp"
#include "nedmalloc.h"


#define HEADER_SIZE 128

namespace Journal{

typedef boost::asio::ip::tcp::socket raw_socket;

class Connection
    :public boost::enable_shared_from_this<Connection>,
     private boost::noncopyable
{
public:
    explicit Connection(raw_socket & socket_,entry_queue& entry_queue);
    virtual ~Connection();
    bool init(nedalloc::nedpool * buffer);
    bool deinit();
    void start();
    void stop();

private:
    void handle_read_header(const boost::system::error_code& error);
    void handle_read_body(char* buffer_ptr,uint32_t buffer_size,const boost::system::error_code& e);
    void handle_write(const boost::system::error_code& e, std::size_t bytes_transferred);
    bool handle_request(char* buffer,uint32_t size,char* header);

    raw_socket& raw_socket_;
    entry_queue& entry_queue_;
    boost::array<char, HEADER_SIZE> header_buffer_;
    nedalloc::nedpool * buffer_pool;

};

typedef boost::shared_ptr<Connection> connection_ptr;
}

#endif
