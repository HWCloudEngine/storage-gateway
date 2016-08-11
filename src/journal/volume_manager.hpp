#ifndef JOURNAL_VOLUME_MANAGER_HPP
#define JOURNAL_VOLUME_MANAGER_HPP

#include <set>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include "connection.hpp"
#include "journal_writer.hpp"
#include "request_handler.hpp"
#include "nedmalloc.h"

#define BUFFER_POOL_SIZE 1024*1024*32

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
private:
    entry_queue write_queue_;
    entry_queue entry_queue_;
    raw_socket raw_socket_;
        
    RequestHandler handler;
    Connection connection;
    JournalWriter writer;

    nedalloc::nedpool * buffer_pool;
};
typedef boost::shared_ptr<Volume> volume_ptr;

class VolumeManager
    :private boost::noncopyable
{
public:
    void start(std::string vol_id,volume_ptr vol);
    void stop(std::string vol_id);
    void stop_all();
private:
    std::map<std::string,volume_ptr> volumes;
};
}

#endif  