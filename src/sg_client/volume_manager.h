#ifndef JOURNAL_VOLUME_MANAGER_H
#define JOURNAL_VOLUME_MANAGER_H
#include <map>
#include <string>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include <sys/epoll.h>
#include "volume.h"
#include "common/rpc_server.h"
#include "common/config_option.h"
#include "rpc/clients/volume_inner_ctrl_client.h"
#include "rpc/clients/writer_client.h"

using namespace std; 

/*forward declaration*/
class SnapshotControlImpl;
class BackupControlImpl;
class VolumeControlImpl;
class ReplicateCtrl;
class AgentControlImpl;
class VolumeControlImpl;

class VolumeManager
{
public:
    VolumeManager(const std::string& host, const std::string& port):
        host_(host), port_(port),running_(true){};
    virtual ~VolumeManager();

    VolumeManager(const VolumeManager& other) = delete;
    VolumeManager& operator=(const VolumeManager& other) = delete;

    void start(raw_socket_t client_sock);
    void stop(std::string vol_id);
    void stop_all();
    bool init();
    bool del_volume(const string& vol);
    bool recover_targets();

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

    // work method of producer marker update thread
    void update_producer_markers(
        std::map<string,JournalMarker>& markers_to_update);

    void writer_thread_work();

    int update_all_producer_markers();

    /*all volumes to be protected*/
    std::map<std::string, shared_ptr<Volume>> volumes;
    /*journal prefetch and seal*/
    std::shared_ptr<WriterClient> writer_rpc_client;
    int_least64_t interval;
    int journal_limit;
    shared_ptr<CephS3LeaseClient> lease_client;
    std::mutex mtx;
    boost::shared_ptr<boost::thread> thread_ptr;
    /*rpc server receive ctrl command from sg controller */
    RpcServer* ctrl_rpc_server{nullptr};

    SnapshotControlImpl* snapshot_ctrl{nullptr};
    BackupControlImpl*   backup_ctrl{nullptr};
    ReplicateCtrl*       rep_ctrl{nullptr};

    /*thread use to recover tgt target*/
    shared_ptr<thread> recover_targets_thr_;
    VolumeControlImpl*   vol_ctrl{nullptr};

    std::string host_;
    std::string port_;
    std::shared_ptr<VolInnerCtrlClient> vol_inner_client_;

    bool running_;

    // producer marker update related
    int epoll_fd;
    // thread to sync producer makers
    std::unique_ptr<std::thread> writer_thread;
    struct epoll_event* ep_events;
    int max_ep_events_num;
    int producer_marker_update_interval;
    // the map to keep last producer markers updated
    std::map<string,JournalMarker> last_producer_markers;
};

#endif  
