/*
 * ceph_s3_lease.cc
 *
 *  Created on: 2016򣌕2�
 *      Author: smile-luobin
 */

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

using huawei::proto::DRS_OK;
using huawei::proto::INTERNAL_ERROR;

RESULT CephS3LeaseClient::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int renew_window,
        int expire_window, int validity_window) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    renew_window_ = renew_window;
    expire_window_ = expire_window;
    validity_window_ = validity_window;
    prefix_ = "/leases/";

    if (acquire_lease()) {
        renew_thread_ptr_.reset(
                new boost::thread(
                        boost::bind(&CephS3LeaseClient::renew_lease, this)));
        check_thread_ptr_.reset(
                new boost::thread(
                        boost::bind(&CephS3LeaseClient::check_lease, this)));

        return DRS_OK;
    } else {
        return INTERNAL_ERROR;
    }
}

std::string& CephS3LeaseClient::get_lease() {
    {
        std::unique_lock<std::mutex> luk(lease_mtx_);
        return uuid_;
    }
}

bool CephS3LeaseClient::acquire_lease() {
    std::string uuid = boost::uuids::to_string(
            boost::uuids::uuid(boost::uuids::random_generator()()));
    long now_time = static_cast<long>(time(NULL));
    std::map<std::string, std::string> metadata;
    {
        std::unique_lock<std::mutex> euk(expire_mtx_);
        lease_expire_time_ = now_time + expire_window_;
        metadata["expire-time"] = std::to_string(lease_expire_time_);
    }
    std::string lease_key = prefix_ + uuid;
    RESULT result = s3Api_ptr_->put_object(lease_key.c_str(), &uuid, &metadata);
    if (result == DRS_OK) {
        {
            std::unique_lock<std::mutex> luk(lease_mtx_);
            uuid_ = uuid;
        }
        return true;
    } else {
        return false;
    }
}

void CephS3LeaseClient::renew_lease() {
    while (true) {
        long now_time = static_cast<long>(time(NULL));
        std::map<std::string, std::string> metadata;
        {
            std::unique_lock<std::mutex> euk(expire_mtx_);
            lease_expire_time_ = now_time + expire_window_;
            metadata["expire-time"] = std::to_string(lease_expire_time_);
        }
        {
            std::unique_lock<std::mutex> luk(lease_mtx_);
            std::string lease_key = prefix_ + uuid_;
            RESULT result = s3Api_ptr_->put_object(lease_key.c_str(), &uuid_,
                    &metadata);
        }
        boost::this_thread::sleep_for(boost::chrono::seconds(renew_window_));
    }
}

void CephS3LeaseClient::check_lease() {
    while (true) {
        long now_time = static_cast<long>(time(NULL));
        long sleep_time = 0;
        {
            std::unique_lock<std::mutex> euk(expire_mtx_);
            sleep_time = lease_expire_time_ - now_time;
        }
        boost::this_thread::sleep_for(boost::chrono::seconds(sleep_time));
        if (check_lease_validity() == false) {
            acquire_lease();
        }
    }
}
bool CephS3LeaseClient::check_lease_validity() {
    long now_time = static_cast<long>(time(NULL));
    {
        std::unique_lock<std::mutex> luk(expire_mtx_);
        if (lease_expire_time_ - now_time > validity_window_) {
            return true;
        } else {
            return false;
        }
    }
}

CephS3LeaseClient::~CephS3LeaseClient() {
    if (renew_thread_ptr_.get()) {
        renew_thread_ptr_->interrupt();
        check_thread_ptr_->interrupt();
        renew_thread_ptr_->join();
        check_thread_ptr_->join();
        renew_thread_ptr_.reset();
        check_thread_ptr_.reset();
    }
}

RESULT CephS3LeaseServer::init(const char* access_key, const char* secret_key,
        const char* host, const char* bucket_name, int gc_interval) {
    s3Api_ptr_.reset(new CephS3Api(access_key, secret_key, host, bucket_name));
    gc_interval_ = gc_interval;
    prefix_ = "/leases/";
    gc_thread_ptr_.reset(
            new boost::thread(boost::bind(&CephS3LeaseServer::gc_task, this)));
}

void CephS3LeaseServer::gc_task() {
    while (true) {
        std::list<std::string> leases;
        RESULT result = s3Api_ptr_->list_objects(NULL, NULL, NULL, -1, &leases);
        if (result == DRS_OK) {
            for (auto lease : leases) {
                std::map<std::string, std::string> metadata;
                s3Api_ptr_->head_object(lease.c_str(), &metadata);
                long now_time = static_cast<long>(time(NULL));
                //delete the expired lease
                if (metadata.find("expire-time") != metadata.end()) {
                    try {
                        long expire_time = std::stol(metadata["expire-time"]);
                        if (expire_time < now_time) {
                            s3Api_ptr_->delete_object(lease.c_str());
                        }
                    } catch (...) {
                    }
                }
            }
        }
        boost::this_thread::sleep_for(boost::chrono::seconds(gc_interval_));
    }
}

bool CephS3LeaseServer::check_lease_existance(const std::string& uuid) {
    std::string value;
    std::map<std::string, std::string> metadata;
    std::string object_key = prefix_ + uuid;
    RESULT result = s3Api_ptr_->head_object(object_key.c_str(), &metadata);

    if (result != DRS_OK) {
        return false;
    } else {
        if (metadata.find("expire-time") != metadata.end()) {
            long now_time = static_cast<long>(time(NULL));
            try {
                long expire_time = std::stol(metadata["expire-time"]);
                if (expire_time < now_time) {
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

CephS3LeaseServer::~CephS3LeaseServer() {
    if (gc_thread_ptr_.get()) {
        gc_thread_ptr_->interrupt();
        gc_thread_ptr_->join();
        gc_thread_ptr_.reset();
    }
}
