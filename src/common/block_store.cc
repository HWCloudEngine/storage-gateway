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
#include <fstream>
#include <assert.h>
#include "log/log.h"
#include "block_store.h"
#include "config_option.h"
#include "define.h"
#include "env_posix.h"

BlockStore* BlockStore::factory(const std::string& type) {
    if (type == "ceph") {
        return new CephBlockStore(g_option.ceph_cluster_name,
                                  g_option.ceph_user_name,
                                  g_option.ceph_pool_name);
    }
    if (type == "local") {
        return new FsBlockStore(g_option.local_data_path);
    }
    return nullptr;
}

CephBlockStore::CephBlockStore(const std::string& cluster,
                               const std::string& user,
                               const std::string& pool) 
    : m_cluster_name(cluster), m_user_name(user), m_pool_name(pool) {
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

int CephBlockStore::create(const std::string& object) {
    return 0;
}

int CephBlockStore::remove(const std::string& object) {
    LOG_INFO << "ceph store remove:" << object;
    return rados_remove(m_io_ctx, object.c_str());
}

int CephBlockStore::write(const std::string& object, char* buf, size_t len, off_t off) {
    /*success return 0*/
    return rados_write(m_io_ctx, object.c_str(), buf, len, off);
}

int CephBlockStore::read(const std::string& object, char* buf, size_t len, off_t off) {
    /*return read size if success*/
    return rados_read(m_io_ctx, object.c_str(), buf, len, off);
}


FsBlockStore::FsBlockStore(const std::string& dir) {
    m_dir = dir;
}

FsBlockStore::~FsBlockStore() {

}

int FsBlockStore::create(const std::string& object) {
    return 0;
}

int FsBlockStore::remove(const std::string& object) {
    std::string abs_obj_dir = m_dir;
    if (-1 != object.find("snap")) {
        abs_obj_dir.append("/snapshot/");
    }
    if (-1 != object.find("backup")) {
        abs_obj_dir.append("/backup/");
    }
    abs_obj_dir.append(object);
    LOG_INFO << "fs store remove:" << object << " abs_dir:" << abs_obj_dir;
    unlink(abs_obj_dir.c_str());
    return 0;
}

std::string FsBlockStore::create_absolute_dir(std::string obj_name) {
    /*success return 0*/
    std::string abs_obj_dir = m_dir;
    if (-1 != obj_name.find("snap")) {
        abs_obj_dir.append("/snapshot/");
    }
    if (-1 != obj_name.find("backup")) {
        abs_obj_dir.append("/backup/");
    }
    size_t pos = obj_name.find_last_of("/");
    if (pos == -1) {
        LOG_ERROR << "ojecet:" << obj_name << " format no support";
        return "";
    }
    std::string obj_dir = obj_name.substr(0, pos);
    abs_obj_dir.append(obj_dir);
    char cmd[256] = {0};
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", abs_obj_dir.c_str());
    int ret = system(cmd);
    if (ret == -1) {
        LOG_ERROR << "cmd:" << cmd << " exec failed";
        return "";
    }
    LOG_INFO << "obj:" << obj_name << " obj_dir:" << abs_obj_dir;
    return abs_obj_dir; 
}

int FsBlockStore::write(const std::string& object, char* buf, size_t len, off_t off) {
    std::string obj_dir = create_absolute_dir(object);
    if (!obj_dir.empty()) {
        size_t pos = object.find_last_of("/");
        assert(pos != -1);
        std::string file_name = object.substr(pos+1);
        std::string obj_path = obj_dir + "/" + file_name;
        unique_ptr<AccessFile> pfile;
        Env::instance()->create_access_file(obj_path, true, false, &pfile);
        size_t wret = pfile->write(buf, len, off); 
        assert(wret == len);
        LOG_INFO << "write obj_path:" << obj_dir << " file:" << file_name << " obj_path:" << obj_path << " ok";
    }
    return 0;
}

int FsBlockStore::read(const std::string& object, char* buf, size_t len, off_t off) {
    std::string obj_dir = create_absolute_dir(object);
    if (!obj_dir.empty()) {
        size_t pos = object.find_last_of("/");
        assert(pos != -1);
        std::string file_name = object.substr(pos+1);
        std::string obj_path = obj_dir + "/" + file_name;
        unique_ptr<AccessFile> pfile;
        Env::instance()->create_access_file(obj_path, true, false, &pfile);
        size_t rret = pfile->read(buf, len, off); 
        assert(rret == len);
        LOG_INFO << "read obj_path:" << obj_dir << " file:" << file_name << " obj_path:" << obj_path << " ok";
    }
    return len;
}
