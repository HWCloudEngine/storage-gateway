#ifndef VOLUME_H
#define VOLUME_H
#include <atomic>
#include <boost/asio.hpp>
#include "nedmalloc.h"
#include "journal/journal_entry.h"
#include "seq_generator.h"
#include "cache/cache_proxy.h"
#include "connection.h"
#include "journal_preprocessor.h"
#include "journal_writer.h"
#include "journal_reader.h"
#include "journal_replayer.h"
#include "../common/config_parser.h"
#include "../common/blocking_queue.h"
#include "../common/volume_attr.h"
#include "../common/ceph_s3_lease.h"
#include "../snapshot/snapshot_proxy.h"
#include "../backup/backup_decorator.h"
#include "../backup/backup_proxy.h"
#include "../rpc/common.pb.h"

#define BUFFER_POOL_SIZE 1024*1024*64
#define REQUEST_BODY_SIZE 512

using namespace std; 

namespace Journal{

class Volume 
{
public:
    explicit Volume(raw_socket_t client_sock, 
                    const VolumeAttr& vol_attr,
                    shared_ptr<ConfigParser> conf, 
                    shared_ptr<CephS3LeaseClient> lease_client);

    virtual ~Volume();
    
    Volume(const Volume& other) = delete;
    Volume& operator=(const Volume& other) = delete;

    bool init();
    void fini();

    void start();
    void stop();

    JournalWriter& get_writer()const;
    shared_ptr<SnapshotProxy>& get_snapshot_proxy()const;
    shared_ptr<BackupProxy>&   get_backup_proxy()const;
    shared_ptr<ReplicateProxy>& get_replicate_proxy()const;

private:
    /*socket*/
    mutable raw_socket_t raw_socket_;
    /*memory pool for receive io hook data from raw socket*/
    nedalloc::nedpool* buffer_pool_;
    
    VolumeAttr vol_attr_;

    shared_ptr<ConfigParser> conf_;
    shared_ptr<CephS3LeaseClient> lease_client_;

    /*queue */
    BlockingQueue<shared_ptr<JournalEntry>> entry_queue_;  /*connection and preprocessor*/
    BlockingQueue<struct IOHookReply*>      reply_queue_;  /*connection*/
    BlockingQueue<shared_ptr<JournalEntry>> write_queue_;  /*writer*/
    BlockingQueue<struct IOHookRequest>     read_queue_;   /*reader*/
    
    /*cache */
    shared_ptr<IDGenerator> idproxy_;
    shared_ptr<CacheProxy>  cacheproxy_;

    /*snapshot relevant*/
    mutable shared_ptr<SnapshotProxy>   snapshotproxy_;
    mutable shared_ptr<BackupDecorator> backupdecorator_;

    /*backup relevant*/
    mutable shared_ptr<BackupProxy> backupproxy_;
    
    /*replicate proxy */
    mutable shared_ptr<ReplicateProxy> rep_proxy_;
    /*work thread*/ 
    shared_ptr<Connection>           connection_;    /*network receive and send*/
    shared_ptr<JournalPreProcessor>  pre_processor_; /*request merge and crc*/
    mutable shared_ptr<JournalWriter> writer_;        /*append to journal */
    shared_ptr<JournalReader>         reader_;        /*read io*/
    shared_ptr<JournalReplayer>       replayer_;      /*replay journal*/
};

}

#endif  
