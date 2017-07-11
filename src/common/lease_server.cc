#include <map>
#include <chrono>
#include <time.h>
#include <thread>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include "lease_server.h"
#include "../log/log.h"

using huawei::proto::sOk;
using huawei::proto::sInternalError;

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

