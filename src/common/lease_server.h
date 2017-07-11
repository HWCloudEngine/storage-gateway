#ifndef LEASE_SERVER_H_
#define LEASE_SERVER_H_

#include <mutex>
#include <string>
#include <boost/thread/thread.hpp>
#include "kv_api.h"
#include "journal_lease.h"

class CephS3LeaseServer: public LeaseServer {
private:
    int gc_interval_;
    std::string prefix_;
    std::shared_ptr<KVApi> kv_ptr_;
    std::unique_ptr<boost::thread> gc_thread_ptr_;
    void gc_task();
public:
    StatusCode init(std::shared_ptr<KVApi>& kv_ptr, int gc_interval);
    virtual bool check_lease_existance(const std::string&);
    CephS3LeaseServer();
    virtual ~CephS3LeaseServer();
};

#endif /* LEASE_SERVER_H_ */
