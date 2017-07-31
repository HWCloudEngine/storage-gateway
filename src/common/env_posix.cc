/**********************************************
*  Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
*  File name:   config.h 
*  Author: 
*  Date:         2016/11/03
*  Version:      1.0
*  Description: all system configure parameters
*
*************************************************/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include "log/log.h"
#include "env_posix.h"

static constexpr size_t kSectorSize = 512;
static constexpr size_t kPageSize = 4096;

PosixStreamAccessFile::PosixStreamAccessFile(const std::string& fname, FILE* file) 
    : fname_(fname), file_(file) {
    fd_ = fileno(file_);
}

PosixStreamAccessFile::~PosixStreamAccessFile() {
    if (file_) {
        fclose(file_); 
    }
}

ssize_t PosixStreamAccessFile::read(char* buf, const size_t& len) {
    size_t ret = fread(buf, 1, len, file_); 
    if (ret != len) {
        if (feof(file_)) {
            LOG_ERROR << "fname:" << fname_ << " len:" << len << "ret:" << ret << " eof";
        } else {
            LOG_ERROR << "fname:" << fname_ << " len:" << len << "ret:" << ret << " failed";
        }
    }
    return ret;
}

ssize_t PosixStreamAccessFile::read(char* buf, const size_t& size, const off_t& off) {
    int ret = fseek(file_, off, SEEK_SET);
    if (ret == -1) {
        LOG_ERROR << "fseek off:" << off << " failed";
        return ret;
    }
    return read(buf, size);
}

ssize_t PosixStreamAccessFile::readv(const struct iovec* iov, int iovcnt, const off_t& off) {
    off_t  read_off = off;
    size_t read_bytes = 0;
    for (int i = 0 ; i < iovcnt; i++) {
        ssize_t ret = read((char*)iov[i].iov_base, iov[i].iov_len, read_off);
        if (ret != iov[i].iov_len) {
            LOG_ERROR << "readv failed ret:" << ret << " len:" << iov[i].iov_len;
            break;
        }
        read_off += ret;
        read_bytes += ret;
    }
    return read_bytes;
}

ssize_t PosixStreamAccessFile::write(char* buf, size_t size) {
    size_t ret = fwrite(buf, 1, size, file_); 
    if (ret != size) {
        LOG_ERROR << "error fname:" << fname_ << "len:" << size << "ret:" << ret;
    }
    flush();
    return ret;
}

ssize_t PosixStreamAccessFile::write(char* buf, size_t size, off_t off) {
    int ret = fseek(file_, off, SEEK_SET);
    if (ret == -1) {
        LOG_ERROR << "fseek off:" << off << " failed";
        return ret;
    }
    return write(buf, size);
}

ssize_t PosixStreamAccessFile::writev(const struct iovec* iov, int iovcnt, const off_t& off) {
    int ret = fseek(file_, off, SEEK_SET);
    ssize_t write_bytes = 0;
    for (int i = 0; i < iovcnt; i++) {
        ssize_t write_ret = write(reinterpret_cast<char*>(iov[i].iov_base),
                                  iov[i].iov_len);
        if (write_ret != iov[i].iov_len) {
            LOG_ERROR << "writev fname:" << fname_ << "len:" << iov[i].iov_len 
                      << "ret:" << write_ret << " failed";
            break; 
        }
        write_bytes += write_ret;
    }
    return write_bytes;
}

int PosixStreamAccessFile::flush() {
    return fflush(file_);
}

int PosixStreamAccessFile::fadvise(const off_t& off, const size_t& len,
                                   int advice) {
    return posix_fadvise(fd_, off, len, advice);
}

PosixDirectAccessFile::PosixDirectAccessFile(const std::string& fname, int fd) 
    : fname_(fname), fd_(fd) {
}

PosixDirectAccessFile::~PosixDirectAccessFile() {
    if (fd_ != -1) {
        close(fd_);
    }
}

ssize_t PosixDirectAccessFile::read(char* buf, const size_t& len) {
    return ::read(fd_, buf, len);
}

ssize_t PosixDirectAccessFile::read(char* buf, const size_t& size,
                                    const off_t& off) {
    assert(sector_align(size) && sector_align(off));

    char* align_buf = nullptr;
    if (!page_align(buf)) {
        align_buf = reinterpret_cast<char*>(malloc_align(size));
    }

    char* read_buf = page_align(buf) ? buf : align_buf;
    off_t read_off = off;
    size_t bytes_read = 0;

    while (bytes_read < size) {
        ssize_t ret = pread(fd_, read_buf, (size - bytes_read), read_off);
        if (ret <= 0) {
            LOG_ERROR << "read error size:" << (size - bytes_read)
                      << " ret" << ret << " errno:" << errno;
            break;
        }
        read_buf += ret;
        read_off += ret;
        bytes_read += ret;
    }
    
    if (align_buf) {
        memcpy(buf, align_buf, size);
        free(align_buf);
    }
    return bytes_read;
}

ssize_t PosixDirectAccessFile::readv(const struct iovec* iov, int iovcnt,
                                     const off_t& off) {
    return ::preadv(fd_, iov, iovcnt, off);
}

ssize_t PosixDirectAccessFile::write(char* buf, size_t size) {
    return ::write(fd_, buf, size);
}

ssize_t PosixDirectAccessFile::write(char* buf, size_t size, off_t off) {
    assert(sector_align(size) && sector_align(off));

    char* align_buf = nullptr;
    if (!page_align(buf)) {
        align_buf = reinterpret_cast<char*>(malloc_align(size));
        memcpy(align_buf, buf, size);
    }

    char* write_buf = page_align(buf) ? buf : align_buf;
    off_t write_off = off;
    size_t bytes_write = 0;

    while (bytes_write < size) {
        ssize_t ret = ::pwrite(fd_, write_buf, (size - bytes_write), write_off);
        if (ret <= 0) {
            LOG_ERROR << "write error size:" << (size - bytes_write)
                      << " ret" << ret << " errno:" << errno;
            break;
        }
        write_buf += ret;
        write_off += ret;
        bytes_write += ret;
    }

    if (align_buf) {
        free(align_buf);
    }
    return bytes_write;
}

ssize_t PosixDirectAccessFile::writev(const struct iovec* iov, int iovcnt,
                                      const off_t& off) {
    return ::pwritev(fd_, iov, iovcnt, off);
}

int PosixDirectAccessFile::flush() {
    return fsync(fd_);
}

int PosixDirectAccessFile::fadvise(const off_t& off, const size_t& len,
                                   int advice) {
    return posix_fadvise(fd_, off, len, advice);
}

bool PosixDirectAccessFile::sector_align(const off_t& off) {
    return off % kSectorSize == 0;
}

bool PosixDirectAccessFile::page_align(const void* ptr) {
    return uintptr_t(ptr) % kPageSize == 0;
}

void* PosixDirectAccessFile::malloc_align(const size_t& size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, 4096, size)) {
        LOG_ERROR << "posix memalign size:" << size << " failed:" << errno;
        return nullptr;
    }
    return ptr;
}

Env* Env::env = nullptr;

Env* Env::instance() {
    if (env == nullptr) {
        env = new PosixEnv; 
    }
    return env;
}

bool PosixEnv::create_access_file(const std::string& fname, bool direct,
                                  unique_ptr<AccessFile>* ofile) {
    if (!direct) {
        FILE* f = fopen(fname.c_str(), "ab+");
        if (f != nullptr) {
            ofile->reset(new PosixStreamAccessFile(fname, f));
            return true;
        }
        LOG_ERROR << "fopen fname:" << fname << " failed:" << errno;
        return false;
    }
    int fd = open(fname.c_str(), O_RDWR | O_DIRECT);
    if (fd == -1) {
        LOG_ERROR << "open fname:" << fname << " failed:" << errno;
        return false;
    }
    LOG_INFO << "open fname:" << fname << " ok";
    if (ofile) {
        ofile->reset(new PosixDirectAccessFile(fname, fd));
    }
    return true;
}

bool PosixEnv::create_dir(const std::string& dname) {
    if (mkdir(dname.c_str(), 0755) != 0) {
        LOG_ERROR << "mkdir dname:" << dname << " failed " << errno; 
        return false;
    }
    return true;
}

bool PosixEnv::delete_dir(const std::string& dname) {
    if (rmdir(dname.c_str()) != 0) {
        LOG_ERROR << "rmdir dname:" << dname << " failed " << errno; 
        return false;
    }
    return true;
}

bool PosixEnv::get_dirent(const std::string& dname,
                          vector<std::string>* dirents) {
    dirents->clear();
    DIR* d = opendir(dname.c_str());
    if (d == nullptr) {
        LOG_ERROR << "opendir dname:" << dname << " failed " << errno; 
        return false;
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0) {
            continue; 
        } 
        dirents->push_back(entry->d_name);
    }
    return true;
}

bool PosixEnv::file_exists(const std::string& fname) {
    if (access(fname.c_str(), F_OK) != 0) {
        LOG_ERROR << "acces fname:" << fname << " failed " << errno; 
        return false;
    }
    return true;
}

bool PosixEnv::delete_file(const std::string& fname) {
    if (unlink(fname.c_str()) != 0) {
        LOG_ERROR << "unlink fname:" << fname << " failed " << errno; 
        return false;
    }
    return true;
}

size_t PosixEnv::file_size(const std::string& fname) {
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
        LOG_ERROR << "stat fname:" << fname << " failed " << errno; 
        return 0;
    }
    
    if ((sbuf.st_mode & S_IFMT) == S_IFREG) {
        return sbuf.st_size; 
    }

    if ((sbuf.st_mode & S_IFBLK) == S_IFBLK) {
        int fd = open(fname.c_str(), O_RDONLY);
        if (fd != -1) {
            size_t size = 0;
            int ret = ioctl(fd, BLKGETSIZE64, &size);
            if (ret == 0) {
                close(fd);
                LOG_INFO << "file:" << fname << " size:" << size;
                return size;
            }
        }
    }
    return 0;
}

uint64_t PosixEnv::now_micros() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return (tv.tv_sec*1000000000) + tv.tv_usec;
}

void PosixEnv::sleep(uint64_t micros) {
    usleep(micros);
}
