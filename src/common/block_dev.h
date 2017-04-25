#ifndef BLOCK_DEV_H
#define BLOCK_DEV_H
#include <unistd.h>
#include <string>
using namespace std;

class BlockDevice {
 public:
    BlockDevice(string path);
    ~BlockDevice();

    ssize_t sync_write(const char* buf, size_t len, off_t off);
    ssize_t sync_read(char* buf, size_t len, off_t off);
    ssize_t aio_write(const char* buf, size_t len, off_t off, void* ioctx);
    ssize_t aio_read(char* buf, size_t lne, off_t off, void* ioctx);

    int dev_open();
    int dev_fsync();
    int dev_close();
    size_t dev_size();

 private:
    string path_;
    int    fd_;
};

#endif
