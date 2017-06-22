#ifndef CEPH_S3_LEASE_H_
#define CEPH_S3_LEASE_H_

#include <mutex>
#include <string>
#include <boost/thread/thread.hpp>
#include "kv_api.h"
#include "journal_lease.h"

class CephS3LeaseClient: public LeaseClient {
private:
    std::shared_ptr<KVApi> kv_ptr_;
    std::unique_ptr<boost::thread> renew_thread_ptr_;
    std::string uuid_;
    std::string prefix_;
    std::mutex lease_mtx_;

    long lease_expire_time_;
    int expire_window_;
    int renew_window_;
    int validity_window_;

    virtual bool acquire_lease();
    virtual void renew_lease();
public:
    StatusCode init(std::shared_ptr<KVApi>& kv_ptr, int renew_window,
            int expire_window, int validity_window);
    std::string& get_lease();
    virtual bool check_lease_validity(const std::string&);
    CephS3LeaseClient();
    virtual ~CephS3LeaseClient();
};

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

#endif /* CEPH_S3_LEASE_H_ */
