/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:    serversocket.h
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description:  network acceptor module
*
*************************************************/
#ifndef SRC_SG_SERVER_SOCKET_H_
#define SRC_SG_SERVER_SOCKET_H_
#include <iostream>
#include <string>
#include <mutex>
#include <thread>
#include <memory>
#include <vector>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include "common/blocking_queue.h"
#include "message.h"
#include "volume_manager.h"

#ifndef _USE_UNIX_DOMAIN
typedef boost::asio::ip::tcp::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#else
typedef boost::asio::local::stream_protocol::socket socket_t;
typedef shared_ptr<socket_t> raw_socket_t;
#endif

/*io_service pool for asio acceptor*/
class IoServicePool {
 public:
  explicit IoServicePool(std::size_t pool_size);

  void run();

  void stop();

  boost::asio::io_service& get_io_service();

private:
  typedef boost::shared_ptr<boost::asio::io_service> io_service_ptr;
  typedef boost::shared_ptr<boost::asio::io_service::work> work_ptr;

  std::vector<io_service_ptr> io_services_;
  std::vector<work_ptr> work_;
  std::size_t next_io_service_;
};

/*network server*/
class ServerSocket {
 public:
    explicit ServerSocket(const std::string& address,
                          const std::string& port,
                          const std::string& file,
                          uint32_t io_service_pool_size);
    void run();

 private:
    void start_accept();
    void accept_cbt(raw_socket_t client_sock, 
                    const boost::system::error_code& e);
    void handle_stop();

    IoServicePool io_service_pool_;

    #ifndef _USE_UNIX_DOMAIN
    boost::asio::ip::tcp::acceptor acceptor_;
    #else
    boost::asio::local::stream_protocol::acceptor acceptor_;
    #endif

    boost::asio::signal_set signals_;
    
    VolumeManager volume_manager_;
};

#endif  // SRC_SG_SERVER_SOCKET_H_
