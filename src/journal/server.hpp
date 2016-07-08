#ifndef JOURNAL_SERVER_HPP
#define JOURNAL_SERVER_HPP

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <string>
#include "connection.hpp"
#include "volume_manager.hpp"
#include "request_handler.hpp"
#include "io_service_pool.hpp"

namespace Journal{

class Server
    :private boost::noncopyable
{
public:
    explicit Server(const std::string& address, const std::string& port,uint32_t io_service_pool_size);
    void run();

private:
    void start_accept();
    void handle_accept(const boost::system::error_code& e);
    void handle_stop();

    boost::asio::ip::tcp::acceptor acceptor_;

    VolumeManager volume_manager_;
    volume_ptr new_volume_;

    io_service_pool io_service_pool_;
    boost::asio::signal_set signals_;
};
}

#endif      
