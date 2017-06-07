/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    server_socket.cc
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: network acceptor module 
*
*************************************************/
#include <string>
#include <stdexcept>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include "common/define.h"
#include "server_socket.h"

using boost::asio::buffer;

IoServicePool::IoServicePool(std::size_t pool_size) : next_io_service_(0) {
  if (pool_size == 0)
    throw std::runtime_error("io_service_pool size is 0");

  for (std::size_t i = 0; i < pool_size; ++i) {
    io_service_ptr io_service(new boost::asio::io_service);
    work_ptr work(new boost::asio::io_service::work(*io_service));
    io_services_.push_back(io_service);
    work_.push_back(work);
  }
}

void IoServicePool::run() {
  // Create a pool of threads to run all of the io_services.
  std::vector<boost::shared_ptr<boost::thread> > threads;
  for (std::size_t i = 0; i < io_services_.size(); ++i) {
    boost::shared_ptr<boost::thread> thread(new boost::thread(
          boost::bind(&boost::asio::io_service::run, io_services_[i])));
    threads.push_back(thread);
  }

  // Wait for all threads in the pool to exit.
  for (std::size_t i = 0; i < threads.size(); ++i)
    threads[i]->join();
}

void IoServicePool::stop() {
  // Explicitly stop all io_services.
  for (std::size_t i = 0; i < io_services_.size(); ++i)
    io_services_[i]->stop();
}

boost::asio::io_service& IoServicePool::get_io_service() {
  // Use a round-robin scheme to choose the next io_service to use.
  boost::asio::io_service& io_service = *io_services_[next_io_service_];
  ++next_io_service_;
  if (next_io_service_ == io_services_.size())
    next_io_service_ = 0;
  return io_service;
}

ServerSocket::ServerSocket(const std::string& address, const std::string& port,
                           const std::string& file, uint32_t io_service_pool_size)
    :io_service_pool_(io_service_pool_size),
    signals_(io_service_pool_.get_io_service()),
    #ifndef _USE_UNIX_DOMAIN
    acceptor_(io_service_pool_.get_io_service()),
    #else
    acceptor_(io_service_pool_.get_io_service(), boost::asio::local::stream_protocol::endpoint(file)),
    #endif
    volume_manager_(address, port) {
    signals_.add(SIGINT);
    signals_.add(SIGTERM);
    #if defined(SIGQUIT)
    signals_.add(SIGQUIT);
    #endif 
    signals_.async_wait(boost::bind(&ServerSocket::handle_stop, this));

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

    volume_manager_.recover_targets();
}

void ServerSocket::run() {
    io_service_pool_.run();
}

void ServerSocket::start_accept() {
    raw_socket_t client_sock(new socket_t(io_service_pool_.get_io_service()));
    LOG_INFO << "ready to accept client connection";
    acceptor_.async_accept(*client_sock,
        boost::bind(&ServerSocket::accept_cbt, this, 
                    client_sock, boost::asio::placeholders::error));
}

void ServerSocket::accept_cbt(raw_socket_t client_sock, 
                              const boost::system::error_code & e) {
    if(!acceptor_.is_open()){
        LOG_ERROR << "handle accept failed no open";
        return;
    }
    
    if(e){
        LOG_ERROR << "handle accept error e:"  << e;
        return;
    }

    LOG_INFO << "now accept new client connect ok";
    
    /*notify volume manager to add volume*/
    volume_manager_.start(client_sock);
    
    /*start receive another client connect*/
    start_accept();
}

void ServerSocket::handle_stop() {
    acceptor_.close();
    volume_manager_.stop_all();
    io_service_pool_.stop();
}
