#include <map>
#include <chrono>
#include <time.h>
#include <thread>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "ceph_s3_lease.h"
#include "../log/log.h"

using huawei::proto::sOk;
using huawei::proto::sInternalError;

StatusCode CephS3LeaseClient::init(std::shared_ptr<KVApi>& kv_ptr, int renew_window,
        int expire_window, int validity_window) {
    kv_ptr_ = kv_ptr;
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

    StatusCode result = kv_ptr_->put_object(lease_key.c_str(), &uuid, &metadata);
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

            StatusCode result = kv_ptr_->put_object(lease_key.c_str(), &uuid_,
                    &metadata);
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

StatusCode CephS3LeaseServer::init(std::shared_ptr<KVApi>& kv_ptr, int gc_interval) {
    kv_ptr_ = kv_ptr;
    gc_interval_ = gc_interval;
    prefix_ = "/leases/";
    gc_thread_ptr_.reset(
            new boost::thread(boost::bind(&CephS3LeaseServer::gc_task, this)));
    return sOk;
}

void CephS3LeaseServer::gc_task() {
    while (true) {
        std::list<std::string> leases;
        StatusCode result = kv_ptr_->list_objects(prefix_.c_str(), NULL, -1, &leases);
        if (result == sOk) {
            for (auto lease : leases) {
                std::map<std::string, std::string> metadata;
                StatusCode result = kv_ptr_->head_object(lease.c_str(),
                        &metadata);
                if (result == sOk) {
                    if (metadata.find("expire-window") != metadata.end()) {
                        try {
                            long expire_window = std::stol(
                                    metadata["expire-window"]);
                            long last_modified = std::stol(
                                    metadata["last-modified"]);
                            long expire_time = last_modified + expire_window;
                            long now_time = static_cast<long>(time(NULL));

                            if (expire_time <= now_time) {
                                kv_ptr_->delete_object(lease.c_str());
                            }
                        } catch (...) {
                        }
                    }
                }
            }
        }
        boost::this_thread::sleep_for(boost::chrono::seconds(gc_interval_));
    }
}

bool CephS3LeaseServer::check_lease_existance(const std::string& uuid) {
    std::map<std::string, std::string> metadata;
    std::string object_key = prefix_ + uuid;
    StatusCode result = kv_ptr_->head_object(object_key.c_str(), &metadata);

    if (result != sOk) {
        return false;
    } else {
        if (metadata.find("expire-window") != metadata.end()) {
            try {
                long expire_window = std::stol(metadata["expire-window"]);
                long last_modified = std::stol(metadata["last-modified"]);
                long expire_time = last_modified + expire_window;
                long now_time = static_cast<long>(time(NULL));

                if (expire_time <= now_time) {
                    return false;
                } else {
                    return true;
                }
            } catch (...) {
                return true;
            }
        } else {
            return true;
        }
    }
}

CephS3LeaseServer::CephS3LeaseServer() {
    gc_interval_ = 0;
}

CephS3LeaseServer::~CephS3LeaseServer() {
    if (gc_thread_ptr_.get()) {
        gc_thread_ptr_->interrupt();
        gc_thread_ptr_->join();
        gc_thread_ptr_.reset();
    }
}

