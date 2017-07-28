#include <iostream>
#include <string>
#include <signal.h>
#include "log/log.h"
#include "server_socket.h"
#include "common/config_option.h"

int main(int argc, char* argv[]) {
    signal(SIGSEGV, signal_handler);
    assert(argc == 1);
    //todo read from config file
    uint32_t io_service_pool_size = 1;
    std::remove(argv[3]);
    DRLog::log_init("sg_client.log");
    try {
        ServerSocket server(g_option.io_server_ip,
                            std::to_string(g_option.io_server_port),
                            g_option.io_server_uds, io_service_pool_size);
        server.run();
    } catch (std::exception& e) {
        std::cerr <<"exception:" << e.what() << "\n";
    }
    return 0;
}
