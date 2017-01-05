#ifndef JOURNAL_SERVER_HPP
#define JOURNAL_SERVER_HPP

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <string>
#include "connection.hpp"
#include "volume_manager.hpp"
#include "io_service_pool.hpp"

namespace Journal{

class Server : private boost::noncopyable
{
public:
    explicit Server(const std::string& address,
                    const std::string& port,
                    const std::string& file,
                    uint32_t io_service_pool_size);
    void run();

private:
    void start_accept();
    void handle_accept(raw_socket_t client_sock, 
                       const boost::system::error_code& e);
    void handle_stop();

    io_service_pool io_service_pool_;

    #ifndef _USE_UNIX_DOMAIN
    boost::asio::ip::tcp::acceptor acceptor_;
    #else
    boost::asio::local::stream_protocol::acceptor acceptor_;
    #endif

    boost::asio::signal_set signals_;
    
    VolumeManager volume_manager_;
};

}

#endif      
