#ifndef JOURNAL_VOLUME_MANAGER_HPP
#define JOURNAL_VOLUME_MANAGER_HPP
#include <map>
#include <string>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include "volume.hpp"

using namespace std; 

/*forward declaration SnapshotSvc*/                                              
class ControlService;  

namespace Journal{

class VolumeManager
{
public:
    VolumeManager() = default;
    virtual ~VolumeManager();

    VolumeManager(const VolumeManager& other) = delete;
    VolumeManager& operator=(const VolumeManager& other) = delete;

    void start(raw_socket_t client_sock);
    void stop(std::string vol_id);
    void stop_all();
    bool init();

private:
    /*todo periodic task manage journal should put into each volume */
    void periodic_task();
    
    /*boost asio read request header callback function*/
    void read_req_head_cbt(raw_socket_t client_sock,
                           const char* req_head_buffer,
                           const boost::system::error_code& e);
    
    /*boost asio read request body callback function*/
    void read_req_body_cbt(raw_socket_t client_sock,
                           const char* req_head_buffer,
                           const char* req_body_buffer,
                           const boost::system::error_code& e);

    /*boost asio send reply*/
    void send_reply(raw_socket_t client_sock, 
                    const char* req_head_buffer, 
                    const char* req_body_buffer, bool success);

    /*boost asio send reply callback function*/
    void send_reply_cbt(const char* req_head_buffer,
                        const char* req_body_buffer,
                        const char* rep_buffer,
                        const boost::system::error_code& error);
    
    /*all volumes to be protected*/
    std::map<std::string,volume_ptr> volumes;
    
    /*journal prefetch and seal*/
    int_least64_t interval;
    int journal_limit;
    shared_ptr<CephS3LeaseClient> lease_client;
    std::mutex mtx;
    boost::shared_ptr<boost::thread> thread_ptr;
 
    shared_ptr<ConfigParser> conf;

    /*control rpc service*/
    ControlService*  control_service;
};

}

#endif  
