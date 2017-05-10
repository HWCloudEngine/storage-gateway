/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   block_store.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: object storage for block data 
*
*************************************************/
#include <assert.h>
#include "log/log.h"
#include "block_store.h"

CephBlockStore::CephBlockStore(const std::string& cluster,
                               const std::string& user,
                               const std::string& pool) {
    m_cluster_name = cluster;
    m_user_name = user;
    m_pool_name = pool;
    int ret = init();
    if (ret) {
        LOG_ERROR << "CephBlockStore init failed";
    }
}

CephBlockStore::~CephBlockStore() {
    fini();
}

int CephBlockStore::init() {
    uint64_t flags;
    int err;
    err = rados_create2(&m_cluster_ctx, m_cluster_name.c_str(),
                        m_user_name.c_str(), flags);
    if (err < 0) {
        LOG_ERROR << "rados create err:" << err << " errno:" << errno;
        return -1;
    }

    err = rados_conf_read_file(m_cluster_ctx, "/etc/ceph/ceph.conf");
    if (err < 0) {
        LOG_ERROR << "rados conf err:" << err << " errno:" << errno;
        return -1;
    }

    err = rados_connect(m_cluster_ctx);
    if (err < 0) {
        LOG_ERROR << "rados connect err:" << err << " errno:" << errno;
        return -1;
    }

    int64_t pool_id = rados_pool_lookup(m_cluster_ctx, m_pool_name.c_str());
    if (pool_id <= 0) {
        LOG_ERROR << "rados lookup pool:" << m_pool_name << " not exist";
        err = rados_pool_create(m_cluster_ctx, m_pool_name.c_str());
        if (err < 0) {
            LOG_ERROR << "rados create pool:" << m_pool_name << " failed";
            return -1;
        }
    }

    err = rados_ioctx_create(m_cluster_ctx, m_pool_name.c_str(), &m_io_ctx);
    if (err < 0) {
        LOG_ERROR << "rados ioctx create err:" << err << " errno:" << errno;
        return -1;
    }
    return 0;
}

void CephBlockStore::fini() {
    rados_ioctx_destroy(m_io_ctx);
    rados_shutdown(m_cluster_ctx);
}

int CephBlockStore::create(std::string object) {
    return 0;
}

int CephBlockStore::remove(std::string object) {
    return rados_remove(m_io_ctx, object.c_str());
}

int CephBlockStore::write(std::string object, char* buf, size_t len, off_t off) {
    /*success return 0*/
    return rados_write(m_io_ctx, object.c_str(), buf, len, off);
}

int CephBlockStore::read(std::string object, char* buf, size_t len, off_t off) {
    /*return read size if success*/
    return rados_read(m_io_ctx, object.c_str(), buf, len, off);
}
