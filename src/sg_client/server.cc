#include "server.h"
#include <boost/bind.hpp>

using namespace std;

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
    acceptor_(io_service_pool_.get_io_service(), boost::asio::local::stream_protocol::endpoint(file)),
    #endif
    volume_manager_(address, port)
{
    signals_.add(SIGINT);
    signals_.add(SIGTERM);
    #if defined(SIGQUIT)
    signals_.add(SIGQUIT);
    #endif 
    signals_.async_wait(boost::bind(&Server::handle_stop, this));

    volume_manager_.init();

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
    raw_socket_t client_sock(new socket_t(io_service_pool_.get_io_service()));
    LOG_INFO << "Aready to accept client connection";
    acceptor_.async_accept(*client_sock,
        boost::bind(&Server::handle_accept, this, 
                    client_sock, boost::asio::placeholders::error));
}

void Server::handle_accept(raw_socket_t client_sock, 
                           const boost::system::error_code & e)
{
    if(!acceptor_.is_open()){
        LOG_ERROR << "handle accept failed no open";
        return;
    }
    
    if(e){
        LOG_ERROR << "handle accept error e:"  << e;
        return;
    }

    LOG_INFO << "Now accept new client connect ok";
    
    /*notify volume manager to add volume*/
    volume_manager_.start(client_sock);
    
    /*start receive another client connect*/
    start_accept();
}

void Server::handle_stop()
{
    acceptor_.close();
    volume_manager_.stop_all();
    io_service_pool_.stop();
}

}
