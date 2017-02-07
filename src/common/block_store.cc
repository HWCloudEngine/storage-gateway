#include <assert.h>
#include "block_store.h"
#include "../log/log.h"

CephBlockStore::CephBlockStore()
{
    m_cluster_name = "ceph";
    m_user_name    = "client.admin";
    /*todo: pool should auto create*/
    m_pool_name    = "mypool";

    init();
}

CephBlockStore::~CephBlockStore()
{
    fini();
}

int CephBlockStore::init()
{
    uint64_t flags;
    int err;
    err = rados_create2(&m_cluster_ctx, m_cluster_name.c_str(), 
                         m_user_name.c_str(), flags);
    if(err < 0){
        LOG_INFO << "rados create err:" << err << " errno:" << errno; 
        return false;
    }

    err = rados_conf_read_file(m_cluster_ctx, "/etc/ceph/ceph.conf");
    if(err < 0){
        LOG_INFO << "rados conf err:" << err << " errno:" << errno; 
        return false;
    }


    err = rados_connect(m_cluster_ctx);
    if(err < 0){
        LOG_INFO << "rados connect err:" << err << " errno:" << errno; 
        return false;
    }


    err = rados_ioctx_create(m_cluster_ctx, m_pool_name.c_str(), &m_io_ctx);
    if(err < 0){
        LOG_INFO << "rados ioctx create err:" << err << " errno:" << errno; 
        return false;
    }

    return 0;
}

int CephBlockStore::fini()
{
    rados_ioctx_destroy(m_io_ctx);
    rados_shutdown(m_cluster_ctx);

    return 0;
}

int CephBlockStore::create(string object)
{
    return 0;
}

int CephBlockStore::remove(string object)
{
    return rados_remove(m_io_ctx, object.c_str());
}

int CephBlockStore::write(string object, char* buf, size_t len, off_t off)
{
    /*success return 0*/
    return rados_write(m_io_ctx, object.c_str(), buf, len, off);
}

int CephBlockStore::read(string object, char* buf, size_t len, off_t off)
{    
    /*return read size if success*/
    return rados_read(m_io_ctx, object.c_str(), buf, len, off);
}
