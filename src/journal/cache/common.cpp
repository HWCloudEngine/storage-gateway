#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <utility>
#include <sys/mman.h>
#include <algorithm>
#include <string.h>
#include "common.h"
#include "../../log/log.h"

CEntry::CEntry(IoVersion seq, off_t bdev_off, size_t bdev_len,
               string jfile, off_t jfile_off)
{
    io_seq  = seq;
    blk_off = bdev_off;
    blk_len = bdev_len;
    journal_file = jfile;
    journal_off  = jfile_off;
    cache_type   = IN_JOURANL;
    journal_entry  = nullptr;
}

CEntry::CEntry(IoVersion seq, off_t bdev_off, size_t bdev_len,
               string jfile, off_t jfile_off, 
               shared_ptr<ReplayEntry> entry)
{
    io_seq  = seq;
    blk_off = bdev_off;
    blk_len = bdev_len;
    journal_file   = jfile;
    journal_off    = jfile_off;
    cache_type     = IN_MEM;
    journal_entry  = entry;
}

CEntry::CEntry(const CEntry& other)
{
    io_seq  = other.io_seq;
    blk_off = other.blk_off;
    blk_len = other.blk_len;
    journal_file = other.journal_file;
    journal_off  = other.journal_off;
    cache_type   = other.cache_type;
    journal_entry  = other.journal_entry;
}

CEntry::CEntry(CEntry&& other)
{
    *this = std::move(other);
}

CEntry& CEntry::operator=(const CEntry& other)
{
    if(this != &other){
        io_seq  = other.io_seq;
        blk_off = other.blk_off;
        blk_len = other.blk_len;
        journal_file = other.journal_file;
        journal_off  = other.journal_off;
        cache_type   = other.cache_type;
        journal_entry = other.journal_entry;
    } 
    return *this;
}

CEntry& CEntry::operator=(CEntry&& other)
{
    if(this != &other){
        io_seq  = other.io_seq;
        blk_off = other.blk_off;
        blk_len = other.blk_len;
        journal_file = other.journal_file;
        journal_off  = other.journal_off;
        cache_type = other.cache_type;
        journal_entry = other.journal_entry;
    }
    return *this;
}

size_t CEntry::get_mem_size()const
{
    /*todo: CEntry memory space whether take ReplayEntry data in consideration,
     *or should consider other member field in CEntry
     */
    size_t size = 0;
    if(cache_type == IN_MEM){
       log_header_t* lh = (log_header_t*)journal_entry->data();
       int off_count = lh->count;
       off_len_t* poff = (off_len_t*)((char*)lh + sizeof(log_header_t));
       for(int i = 0; i < off_count; i++){
           size += poff[i].length; 
       }
    } else {
       ; 
    }
    return size;
}

int IReadFile::fid = 0;

IReadFile::IReadFile(string file, off_t pos, bool eos)
    :m_file(file), m_pos(pos),m_eos(eos)
{
}

static inline size_t _cal_data_size(off_len_t* off_len, int count)
{
    size_t data_size = 0;
    for(int i = 0 ; i < count; i++){
        data_size += off_len[i].length;
    }
    return data_size;
}

size_t IReadFile:: read_entry(off_t off, 
                              nedalloc::nedpool* bufpool, 
                              shared_ptr<ReplayEntry>& entry)
{
    log_header_t log_head;
    off_len_t* off_len = NULL;
    char* entry_data = NULL;
    size_t ret = 0;

    do {
        /*read head*/
        memset(&log_head, 0, sizeof(log_head));
        size_t head_size = sizeof(log_head);
        off_t  start = off;
        ret = read(start,(char*)&log_head, head_size);
        if(ret != sizeof(log_head)){
            LOG_ERROR << "read log head failed ret=" << ret;
            ret = -1;
            break;
        }
        start += head_size;

        LOG_DEBUG << "read log head ok type: " << log_head.type 
                  << " count:" << (unsigned)log_head.count;

        /*read off len */
        size_t off_len_size = log_head.count * sizeof(off_len_t);
        off_len = (off_len_t*)nedalloc::nedpmalloc(bufpool, off_len_size);
        ret = read(start, (char*)off_len, off_len_size);
        if(ret != off_len_size){
            LOG_ERROR << "read log off len failed ret=" << ret;
            ret = -1;
            break;
        }
        start += off_len_size;
        LOG_DEBUG << "read log off len ok off:" << off_len->offset 
                  << " len:" << off_len->length;

        /*read data*/
        size_t data_size = _cal_data_size(off_len, log_head.count);
        size_t entry_data_len = head_size + off_len_size + data_size;
        entry_data = (char*)nedalloc::nedpmalloc(bufpool, entry_data_len);
        memcpy(entry_data, &log_head, head_size);
        memcpy(entry_data+head_size, off_len, off_len_size);
        ret = read(start, entry_data+head_size+off_len_size, data_size);
        if(ret != data_size){
            LOG_ERROR << "read log data failed ret=" << ret;
            ret = -1;
            break;
        }
        LOG_DEBUG << "read log data ok datasize:" << data_size;

        /*todo: crc checksum*/

        /*read entry successfullly*/
        entry.reset(new ReplayEntry(entry_data, entry_data_len, 0, bufpool));
        ret = entry_data_len;  
    }while(0);

    if(off_len){
        nedalloc::nedpfree(bufpool, off_len);
    } 

    return ret;
}

size_t IReadFile::write_entry(off_t off, shared_ptr<ReplayEntry>& entry)
{
    char* buf  = (char*)entry->data();
    size_t len = entry->length();
    int ret = write(off, buf, len);
    if(ret != len){
        LOG_ERROR << "write entry failed ret:" << ret << " len:" << len;
    }
}

SyncReadFile::SyncReadFile(string file, off_t pos, bool eos)
    :IReadFile(file, pos, eos)
{
    fid++;
}

SyncReadFile::~SyncReadFile()
{
}

int SyncReadFile::open()
{
    m_fd = ::open(m_file.c_str(), O_RDONLY);
    if(-1 == m_fd){
        LOG_ERROR << "open " << m_file.c_str() << "failed errno:" << errno;
        return -1;
    }
    LOG_INFO << "open file:" << m_file;
    struct stat buf = {0};
    int ret = stat(m_file.c_str(), &buf);
    if(-1 == ret){
        LOG_ERROR << "stat " << m_file.c_str() << "failed errno:" << errno;
        return -1;
    }
    m_size = buf.st_size;
    LOG_INFO << "open file:" << m_file << " size:" << m_size;
    return m_fd;
}

void SyncReadFile::close()
{
    if(-1 != m_fd){
        ::close(m_fd);
    } 
}

size_t SyncReadFile::read(off_t off, char* buf, size_t count)
{
    size_t left = count;
    size_t read = 0;
    while(left > 0){
        int ret = pread(m_fd, buf+read, left, off+read);
        if(ret == -1 || ret == 0){
            LOG_ERROR << "syncreadfile pread fd:" << m_fd 
                      << "left:" << left 
                      << "ret:"  << ret
                      << "errno:" << errno << endl; 
            return ret;
        } 
        left -= ret;
        read += ret;
    }

    m_pos += count;
    return read;
}

size_t SyncReadFile::write(off_t off, char* buf, size_t count)
{
    size_t left  = count;
    size_t write = 0;
    while(left > 0){
        int ret = pwrite(m_fd, buf+write, left, off+write);
        if(ret == -1 || ret == 0){
             LOG_ERROR << "syncreadfile pread fd:" << m_fd 
                       << "left:" << left 
                       << "ret:"  << ret
                       << "errno:" << errno << endl; 
            return ret;
        }
        left  -= ret;
        write += ret;
    }

    return write;
}

int SyncReadFile::hashcode()
{
    //todo: how to geneate hashcode of each file
    return fid;
}

MmapReadFile::MmapReadFile(string file, off_t pos, bool eos)
    :IReadFile(file, pos, eos)
{
    fid++;
}

MmapReadFile::~MmapReadFile()
{
}

int MmapReadFile::open()
{
    if(m_fd != -1){
        LOG_INFO << "open " << m_file.c_str() << "already opened";
        return m_fd;
    }
    m_fd = ::open(m_file.c_str(), O_RDONLY);
    if(-1 == m_fd){
        LOG_ERROR << "open " << m_file.c_str() << "failed errno:" << errno;
        return -1;
    }
    struct stat buf = {0};
    int ret = stat(m_file.c_str(), &buf);
    if(-1 == ret){
        LOG_ERROR << "stat " << m_file.c_str() << "failed errno:" << errno;
        return -1;
    }
    m_size = buf.st_size;

    m_mmap_base = mmap(NULL, m_size-m_pos, PROT_READ, MAP_SHARED, m_fd, m_pos);
    if(m_mmap_base == MAP_FAILED){
        LOG_ERROR << "mmap " << m_file.c_str() << "failed errno:" << errno;
        return -1;
    }
    return 0;
}

void MmapReadFile::close()
{
    if(-1 != m_fd){
        ::close(m_fd);
    } 
    if(m_mmap_base != MAP_FAILED){
        munmap(m_mmap_base, m_size-m_pos);
    }
}

size_t MmapReadFile::read(off_t off, char* buf, size_t count)
{
    size_t copy_size = std::min(count, m_size-off);
    memcpy(buf, (char*)m_mmap_base+off-m_pos, copy_size);
    return copy_size;
}

size_t MmapReadFile::write(off_t off, char* buf, size_t count)
{    
    size_t copy_size = std::min(count, m_size-off);
    memcpy((char*)m_mmap_base+off-m_pos, buf, copy_size);
    return copy_size;
}

int MmapReadFile::hashcode()
{
    //todo: how to geneate hashcode of each file
    return fid;
}

