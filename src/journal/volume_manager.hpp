#ifndef JOURNAL_VOLUME_MANAGER_HPP
#define JOURNAL_VOLUME_MANAGER_HPP

#include <set>
#include <mutex>
#include <condition_variable>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include "connection.hpp"
#include "journal_writer.hpp"
#include "pre_processor.hpp"
#include "nedmalloc.h"

#include "../common/blocking_queue.h"
#include "../common/config_parser.h"

#include "seq_generator.hpp"
#include "cache/cache_proxy.h"
#include "journal_replayer.hpp"
#include "journal_reader.hpp"
#include "../dr_server/ceph_s3_lease.h"
#include "../snapshot/snapshot_proxy.h"

#define BUFFER_POOL_SIZE 1024*1024*64
#define REQUEST_BODY_SIZE 512

using namespace std; 
/*forward declaration SnapshotSvc*/                                              
class ControlSvc;  

namespace Journal{

class Volume
    :public boost::enable_shared_from_this<Volume>,
     private boost::noncopyable
{
public:
    explicit Volume(boost::asio::io_service& io_service);
    virtual ~Volume();
    JournalWriter& get_writer();
    raw_socket& get_raw_socket();
    void start();
    void stop();
    bool init(shared_ptr<ConfigParser> conf, shared_ptr<CephS3LeaseClient> lease_client);
    void set_property(std::string vol_id,std::string vol_path);

    shared_ptr<SnapshotProxy> get_snapshot_proxy(){
        return snapshotproxy;
    }

private:
    void periodic_task();
    raw_socket raw_socket_;
    entry_queue write_queue_;
    entry_queue entry_queue_;
    reply_queue reply_queue_;

    std::condition_variable entry_cv;
    std::condition_variable write_cv;
    std::condition_variable reply_cv;

    /*reader relevant*/
    BlockingQueue<struct IOHookRequest> read_queue;
    
    /*cache relevant*/
    shared_ptr<IDGenerator> idproxy;
    shared_ptr<CacheProxy>  cacheproxy;
    /*snapshot relevant*/
    shared_ptr<SnapshotProxy> snapshotproxy;

    PreProcessor pre_processor;
    Connection connection;

    JournalWriter   writer;   /*handle write io*/
    JournalReader   reader;   /*handle read io*/
    JournalReplayer replayer; /*handle write io replay*/

    nedalloc::nedpool * buffer_pool;

    std::string vol_id_;
    std::string vol_path_;
};
typedef boost::shared_ptr<Volume> volume_ptr;


class VolumeManager
    :private boost::noncopyable
{
public:
    VolumeManager();
    virtual ~VolumeManager();
    void start(volume_ptr vol);
    void stop(std::string vol_id);
    void stop_all();
    bool init();
    void add_vol(volume_ptr vol);
private:
    void periodic_task();
    void handle_request_header(volume_ptr vol,const boost::system::error_code& e);
    void handle_request_body(volume_ptr vol,const boost::system::error_code& e);
    void send_reply(volume_ptr vol,bool success);
    void handle_send_reply(const boost::system::error_code& error);
    std::mutex mtx;
    boost::shared_ptr<boost::thread> thread_ptr;
    std::map<std::string,volume_ptr> volumes;
    boost::array<char, HEADER_SIZE> header_buffer_;
    boost::array<char, 512> body_buffer_;
    boost::array<char, HEADER_SIZE> reply_buffer_;

    int_least64_t interval;
    int journal_limit;
    shared_ptr<CephS3LeaseClient> lease_client;
    shared_ptr<ConfigParser> conf;

    /*control rpc service*/
    ControlSvc*  control_svc_;
};
}

#endif  
