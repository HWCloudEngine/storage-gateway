#include <map>
#include <chrono>
#include <time.h>
#include <thread>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "lease_client.h"
#include "../log/log.h"
#include "rpc/clients/rpc_client.h"

using huawei::proto::sOk;
using huawei::proto::sInternalError;

StatusCode CephS3LeaseClient::init(int renew_window,
        int expire_window, int validity_window) {
    renew_window_ = renew_window;
    expire_window_ = expire_window;
    validity_window_ = validity_window;
    prefix_ = "/leases/";

    if (acquire_lease()) {
        renew_thread_ptr_.reset(
                new boost::thread(
                        boost::bind(&CephS3LeaseClient::renew_lease, this)));
        return sOk;
    } else {
        return sInternalError;
    }
}

std::string& CephS3LeaseClient::get_lease() {
    std::unique_lock<std::mutex> luk(lease_mtx_);
    return uuid_;
}

bool CephS3LeaseClient::acquire_lease() {
    std::string uuid = boost::uuids::to_string(
            boost::uuids::uuid(boost::uuids::random_generator()()));
    std::string lease_key = prefix_ + uuid;
    std::map<std::string, std::string> metadata;
    metadata["expire-window"] = std::to_string(expire_window_);

    StatusCode result = g_rpc_client.update_lease(lease_key, uuid, metadata);
    if (result == sOk) {
        lease_expire_time_ = static_cast<long>(time(NULL)) + expire_window_;
        uuid_ = uuid;
        LOG_INFO<<"acquire lease succeed:"<<uuid;
        return true;
    } else {
        LOG_INFO<<"acquire lease failed.";
        return false;
    }
}

void CephS3LeaseClient::renew_lease() {
    while (true) {
        boost::this_thread::sleep_for(boost::chrono::seconds(renew_window_));
        std::unique_lock<std::mutex> luk(lease_mtx_);
        long now_time = static_cast<long>(time(NULL));
        if (lease_expire_time_ - now_time < validity_window_) {
            acquire_lease();
        } else {
            std::string lease_key = prefix_ + uuid_;
            std::map<std::string, std::string> metadata;
            metadata["expire-window"] = std::to_string(expire_window_);

            StatusCode result = g_rpc_client.update_lease(lease_key,
                                                          uuid_,
                                                          metadata);
            if (result == sOk) {
                lease_expire_time_ = static_cast<long>(time(NULL))
                        + expire_window_;
                LOG_INFO<<"renew lease succeed:"<<uuid_;
            }
        }
    }
}

bool CephS3LeaseClient::check_lease_validity(const std::string& uuid) {
    std::unique_lock<std::mutex> luk(lease_mtx_);
    if (uuid != uuid_) {
        return false;
    } else {
        long now_time = static_cast<long>(time(NULL));
        {
            if (lease_expire_time_ - now_time >= validity_window_) {
                return true;
            } else {
                return false;
            }
        }
    }
}

CephS3LeaseClient::CephS3LeaseClient() {
    lease_expire_time_ = 0;
    expire_window_ = 0;
    renew_window_ = 0;
    validity_window_ = 0;
}

CephS3LeaseClient::~CephS3LeaseClient() {
    if (renew_thread_ptr_.get()) {
        renew_thread_ptr_->interrupt();
        renew_thread_ptr_->join();
        renew_thread_ptr_.reset();
    }
}

