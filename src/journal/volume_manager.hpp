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

#define BUFFER_POOL_SIZE 1024*1024*64

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
    bool init(std::string& vol);
private:
    void periodic_task();
    entry_queue write_queue_;
    entry_queue entry_queue_;
    raw_socket raw_socket_;

    std::condition_variable entry_cv;
    std::condition_variable write_cv;
        
    PreProcessor pre_processor;
    Connection connection;
    JournalWriter writer;

    nedalloc::nedpool * buffer_pool;

    std::string vol_id;
};
typedef boost::shared_ptr<Volume> volume_ptr;


class VolumeManager
    :private boost::noncopyable
{
public:
    VolumeManager();
    virtual ~VolumeManager();
    void start(std::string vol_id,volume_ptr vol);
    void stop(std::string vol_id);
    void stop_all();
    bool init();
private:
    void periodic_task();
    std::mutex mtx;
    boost::shared_ptr<boost::thread> thread_ptr;
    std::map<std::string,volume_ptr> volumes;

    int_least64_t interval;
    int journal_limit;
};
}

#endif  