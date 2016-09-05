#include "server.hpp"
#include <boost/bind.hpp>

namespace Journal{

Server::Server(const std::string& address, 
                  const std::string& port,
                  const std::string& file,
                  uint32_t io_service_pool_size)
    :io_service_pool_(io_service_pool_size),
    signals_(io_service_pool_.get_io_service()),
    #ifndef _USE_UNIX_DOMAIN
    acceptor_(io_service_pool_.get_io_service()),
    #else
    acceptor_(io_service_pool_.get_io_service(), boost::asio::local::stream_protocol::endpoint(file))
    #endif
    volume_manager_(),
    new_volume_()
{
    signals_.add(SIGINT);
    signals_.add(SIGTERM);
    #if defined(SIGQUIT)
    signals_.add(SIGQUIT);
    #endif 
    volume_manager_.init();
    signals_.async_wait(boost::bind(&Server::handle_stop, this));
    #ifndef _USE_UNIX_DOMAIN
    boost::asio::ip::tcp::resolver resolver(acceptor_.get_io_service());
    boost::asio::ip::tcp::resolver::query query(address, port);
    boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(query);
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();
    #endif

    start_accept();
}

void Server::run()
{
    io_service_pool_.run();
}

void Server::start_accept()
{
    new_volume_.reset(new Volume(io_service_pool_.get_io_service()));
    acceptor_.async_accept(new_volume_->get_raw_socket(),
        boost::bind(&Server::handle_accept,this,boost::asio::placeholders::error));
}

void Server::handle_accept(const  boost::system::error_code & e)
{
    if(!acceptor_.is_open())
    {
        return;
    }
    
    if(!e)
    {
        volume_manager_.start(new_volume_);
    }
    else
    {
        LOG_ERROR << "Can not handle this accept";
    }
    start_accept();
}

void Server::handle_stop()
{
    acceptor_.close();
    volume_manager_.stop_all();
    io_service_pool_.stop();
}

}
