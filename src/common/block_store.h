#ifndef _BLOCK_STORE_H
#define _BLOCK_STORE_H
#include <unistd.h>
#include <string>
#include <rados/librados.h>

using namespace std;

/*store cow data*/
class BlockStore
{
public:
    BlockStore(){}
    virtual ~BlockStore(){}
    
    virtual int create(string object) = 0;
    virtual int remove(string object) = 0;
    virtual int write(string object, char* buf, size_t len, off_t off) = 0;
    virtual int read(string object, char* buf, size_t len, off_t off) = 0;
};

class CephBlockStore : public BlockStore
{
public:
    CephBlockStore();
    virtual ~CephBlockStore();
    
    int init();
    int fini();

    int create(string object) override;
    int remove(string object) override;
    int write(string object, char* buf, size_t len, off_t off) override;
    int read(string object, char* buf, size_t len, off_t off) override;

private:
    string        m_cluster_name;
    string        m_user_name;
    string        m_pool_name;

    rados_t       m_cluster_ctx;
    rados_ioctx_t m_io_ctx;
};

#endif
