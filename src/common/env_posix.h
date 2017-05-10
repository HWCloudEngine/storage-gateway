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
#ifndef SRC_COMMON_ENV_POSIX_H_
#define SRC_COMMON_ENV_POSIX_H_
#include <unistd.h>
#include <stdio.h>
#include <sys/uio.h>
#include <memory>
#include <vector>

using std::unique_ptr;
using std::vector;

/*random read*/
class AccessFile {
 public:
     AccessFile() {}
     virtual ~AccessFile() {}
     virtual ssize_t read(char* buf, const size_t& len) = 0;
     virtual ssize_t read(char* buf, const size_t& size, const off_t& off) = 0;
     virtual ssize_t readv(const struct iovec* iov, int iovcnt, const off_t& off) = 0;
     virtual ssize_t write(char* buf, size_t size) = 0;
     virtual ssize_t write(char* buf, size_t size, off_t off) = 0;
     virtual ssize_t writev(const struct iovec* iov, int iovcnt, const off_t& off) = 0;
     virtual int flush() = 0;
     virtual int fadvise(const off_t& off, const size_t& len, int advice) = 0;
};

class PosixStreamAccessFile : public AccessFile {
 public:
     PosixStreamAccessFile(const std::string& fname, FILE* file);
     ~PosixStreamAccessFile();
     ssize_t read(char* buf, const size_t& len) override;
     ssize_t read(char* buf, const size_t& size, const off_t& off) override;
     ssize_t readv(const struct iovec* iov, int iovcnt, const off_t& off) override;
     ssize_t write(char* buf, size_t size) override;
     ssize_t write(char* buf, size_t size, off_t off) override;
     ssize_t writev(const struct iovec* iov, int iovcnt, const off_t& off) override;
     int flush() override;
     int fadvise(const off_t& off, const size_t& len, int advice) override;
 private:
     std::string fname_;
     FILE* file_;
     int fd_;
};

class PosixDirectAccessFile : public AccessFile {
 public:
     PosixDirectAccessFile(const std::string& fname, int fd);
     ~PosixDirectAccessFile();
     ssize_t read(char* buf, const size_t& len) override;
     ssize_t read(char* buf, const size_t& size, const off_t& off) override;
     ssize_t readv(const struct iovec* iov, int iovcnt, const off_t& off) override;
     ssize_t write(char* buf, size_t size) override;
     ssize_t write(char* buf, size_t size, off_t off) override;
     ssize_t writev(const struct iovec* iov, int iovcnt, const off_t& off) override;
     int flush() override;
     int fadvise(const off_t& off, const size_t& len, int advice) override;

 private:
     bool sector_align(const off_t& off);
     bool page_align(const void* ptr);
     void* malloc_align(const size_t& size);
 private:
    std::string fname_;
    int fd_;
};

class Env {
 public:
    Env() {}
    virtual ~Env() {}
    static Env* instance();
    static Env* env;
    virtual bool create_access_file(const std::string& fname, bool direct,
                                    unique_ptr<AccessFile>* ofile) = 0;
    virtual bool create_dir(const std::string& dname) = 0;
    virtual bool delete_dir(const std::string& dname) = 0;
    virtual bool get_dirent(const std::string& dname,
                            vector<std::string>* dirents) = 0;
    virtual bool file_exists(const std::string& fname) = 0;
    virtual bool delete_file(const std::string& fname) = 0;
    virtual size_t file_size(const std::string& fname) = 0;

    uint64_t now_micros();
    void sleep(uint64_t micro);
};

class PosixEnv : public Env {
 public:
     PosixEnv() {}
     ~PosixEnv() {}

 public:
    bool create_access_file(const std::string& fname, bool direct,
                            unique_ptr<AccessFile>* ofile) override;
    bool create_dir(const std::string& dname) override;
    bool delete_dir(const std::string& dname) override;
    bool get_dirent(const std::string& dname,
                    vector<std::string>* dirents) override;
    bool file_exists(const std::string& fname) override;
    bool delete_file(const std::string& fname) override;
    size_t file_size(const std::string& fname) override;
};

#endif  // SRC_COMMON_ENV_POSIX_H_
