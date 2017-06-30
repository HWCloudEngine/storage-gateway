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
#ifndef SRC_COMMON_BLOCK_STORE_H_
#define SRC_COMMON_BLOCK_STORE_H_
#include <unistd.h>
#include <string>
#include <rados/librados.h>

/*store cow data*/
class BlockStore {
 public:
    BlockStore() {
    }
    virtual ~BlockStore() {
    }
    
    static BlockStore* factory(const std::string& type);

    virtual int create(const std::string& object) = 0;
    virtual int remove(const std::string& object) = 0;
    virtual int write(const std::string& object, char* buf, size_t len, off_t off) = 0;
    virtual int read(const std::string& object, char* buf, size_t len, off_t off) = 0;
};

class CephBlockStore : public BlockStore {
 public:
    CephBlockStore(const std::string& cluster, const std::string& user,
                   const std::string& pool);
    virtual ~CephBlockStore();

    int init();
    void fini();

    int create(const std::string& object) override;
    int remove(const std::string& object) override;
    int write(const std::string& object, char* buf, size_t len, off_t off) override;
    int read(const std::string& object, char* buf, size_t len, off_t off) override;

 private:
    std::string m_cluster_name;
    std::string m_user_name;
    std::string m_pool_name;
    rados_t m_cluster_ctx;
    rados_ioctx_t m_io_ctx;
};

class FsBlockStore : public BlockStore {
 public:
     explicit FsBlockStore(const std::string& dir);
     virtual ~FsBlockStore();
     int create(const std::string& object) override;
     int remove(const std::string& object) override;
     int write(const std::string& object, char* buf, size_t len, off_t off) override;
     int read(const std::string& object, char* buf, size_t len, off_t off) override;
 private:
     std::string create_absolute_dir(std::string obj_name);
 private:
     std::string m_dir;
};

#endif  // SRC_COMMON_BLOCK_STORE_H_
