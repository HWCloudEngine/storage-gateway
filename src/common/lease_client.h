#ifndef LEASE_CLIENT_H_
#define LEASE_CLIENT_H_

#include <mutex>
#include <string>
#include <boost/thread/thread.hpp>
#include "journal_lease.h"
#include "rpc/common.pb.h"
using huawei::proto::StatusCode;

class CephS3LeaseClient: public LeaseClient {
private:
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
    StatusCode init(int renew_window,
            int expire_window, int validity_window);
    std::string& get_lease();
    virtual bool check_lease_validity(const std::string&);
    CephS3LeaseClient();
    virtual ~CephS3LeaseClient();
};

#endif /* LEASE_CLIENT_H_ */
