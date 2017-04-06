#include <unistd.h>
#include <sys/types.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <errno.h>
#include "log/log.h"
#include "block_dev.h"

BlockDevice::BlockDevice(string path)
{
    path_ = path;
    dev_open();
}

BlockDevice::~BlockDevice()
{
    dev_fsync();
    dev_close();
}

int BlockDevice::dev_open()
{
    fd_ = ::open(path_.c_str(), O_WRONLY | O_DIRECT | O_SYNC);
    if (fd_ < 0){
        LOG_ERROR << "open block device:" << path_ << "failed errno:" << errno;
        return -1;;
    }
    return 0;
}

ssize_t BlockDevice::sync_write(const char* buf, size_t len, off_t off)
{
    return pwrite(fd_, buf, len, off);
}

ssize_t BlockDevice::sync_read(char* buf, size_t len, off_t off)
{
    return pread(fd_, buf, len, off);
}

ssize_t BlockDevice::aio_write(const char* buf, size_t len, off_t off, void* ioctx)
{
    return 0;
}

ssize_t BlockDevice::aio_read(char* buf, size_t lne, off_t off, void* ioctx)
{
    return 0;
}

int BlockDevice::dev_fsync()
{
    return fsync(fd_);
}

int BlockDevice::dev_close()
{
    return ::close(fd_);
}

size_t BlockDevice::dev_size()
{
    size_t block_num = 0;
    ioctl(fd_, BLKGETSIZE, &block_num);
    return block_num * 512;
}
