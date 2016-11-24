#ifndef JOURNAL_VOLUME_MANAGER_HPP
#define JOURNAL_VOLUME_MANAGER_HPP
#include <set>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/thread.hpp>
#include "../common/config_parser.h"
#include "../common/blocking_queue.h"
#include "journal_entry.hpp"
#include "seq_generator.hpp"
#include "cache/cache_proxy.h"
#include "connection.hpp"
#include "pre_processor.hpp"
#include "journal_writer.hpp"
#include "journal_reader.hpp"
#include "journal_replayer.hpp"
#include "nedmalloc.h"
#include "../dr_server/ceph_s3_lease.h"
#include "../snapshot/snapshot_proxy.h"

#define BUFFER_POOL_SIZE 1024*1024*64
#define REQUEST_BODY_SIZE 512

using namespace std; 
/*forward declaration SnapshotSvc*/                                              
class ControlService;  

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
    /*socket*/
    raw_socket raw_socket_;

    /*queue */
    BlockingQueue<shared_ptr<JournalEntry>> entry_queue;  /*connection and preprocessor*/
    BlockingQueue<struct IOHookReply*>  reply_queue;  /*connection*/
    BlockingQueue<shared_ptr<JournalEntry>>   write_queue;  /*writer*/
    BlockingQueue<struct IOHookRequest> read_queue;   /*reader*/
    
    /*cache */
    shared_ptr<IDGenerator> idproxy;
    shared_ptr<CacheProxy>  cacheproxy;
    
    /*work thread*/ 
    Connection   connection;    /*network receive and send*/
    PreProcessor pre_processor; /*request merge and crc*/
    JournalWriter   writer;     /*append to journal */
    JournalReader   reader;     /*read io*/
    JournalReplayer replayer;   /*replay journal*/
    
    /*snapshot relevant*/
    shared_ptr<SnapshotProxy> snapshotproxy;

    /*memory pool for receive io hook data*/
    nedalloc::nedpool * buffer_pool;

    std::string vol_id_;
    std::string vol_path_;
};

typedef shared_ptr<Volume> volume_ptr;


class VolumeManager : private boost::noncopyable
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

   /*receive add or delete volume command*/
    boost::array<char, HEADER_SIZE> header_buffer_;
    boost::array<char, 512> body_buffer_;
    boost::array<char, HEADER_SIZE> reply_buffer_;
    
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
