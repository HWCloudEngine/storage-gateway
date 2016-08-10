#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <utility>
#include <sys/mman.h>
#include <algorithm>
#include <string.h>
#include "common.h"

CEntry::CEntry(IoVersion seq, string file, off_t offset)
{
    log_seq    = seq;
    log_file   = file;
    log_offset = offset;
    cache_type = IN_LOG;
    log_entry  = nullptr;
}

CEntry::CEntry(IoVersion seq, string file, off_t offset, 
               shared_ptr<ReplayEntry> entry)
{
    log_seq    = seq;
    log_file   = file;
    log_offset = offset;
    cache_type = IN_MEM;
    log_entry  = entry;
}

CEntry::CEntry(const CEntry& other)
{
    log_seq = other.log_seq;
    log_file = other.log_file;
    log_offset = other.log_offset;
    cache_type = other.cache_type;
    log_entry = other.log_entry;
}

CEntry::CEntry(CEntry&& other)
{
    *this = std::move(other);
}

CEntry& CEntry::operator=(const CEntry& other)
{
    if(this != &other){
        log_seq = other.log_seq;
        log_file = other.log_file;
        log_offset = other.log_offset;
        cache_type = other.cache_type;
    } 
    return *this;
}

CEntry& CEntry::operator=(CEntry&& other)
{
    if(this != &other){
        log_seq = other.log_seq;
        log_file = other.log_file;
        log_offset = other.log_offset;
        cache_type = other.cache_type;
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
       log_header_t* lh = (log_header_t*)log_entry->data();
       int off_count = lh->count;
       off_len_t* poff = (off_len_t*)((char*)lh + sizeof(log_header_t));
       for(int i = 0; i < off_count; i++){
           size += poff[i].length; 
       }
    } else {
        
    }

    return size;
}

int IReadFile::fid = 0;

IReadFile::IReadFile(string file, off_t pos, bool eos)
    :m_file(file), m_pos(pos),m_eos(eos)
{
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
        cout << "open " << m_file.c_str() << "failed errno:" << errno << endl;
        return -1;
    }
    struct stat buf;
    int ret = fstat(m_fd, &buf);
    if(-1 == ret){
        cout << "stat " << m_file.c_str() << "failed errno:" << errno << endl;
        return -1;
    }
    m_size = buf.st_size;
}

void SyncReadFile::close()
{
    if(-1 != m_fd){
        ::close(m_fd);
    } 
}

size_t SyncReadFile::read(off_t off, char* buf, size_t count)
{
    if(off < m_pos || (off+count) > m_size){
        cout << "read para error off: " << off << " pos:" << m_pos 
             << " size:" << m_size << endl;
        return 0;
    }

    size_t left = count;
    size_t read = 0;
    while(left > 0){
        int ret = pread(m_fd, buf+read, left, off+read);
        if(ret == -1 | ret == 0){
            cout << "pread left:" << left << "failed errno:" << errno << endl; 
            return ret;
        } 

        left -= ret;
        read += ret;
    }

    m_pos += count;
    return read;
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
    m_fd = ::open(m_file.c_str(), O_RDONLY);
    if(-1 == m_fd){
        cout << "open " << m_file.c_str() << "failed errno:" << errno << endl;
        return -1;
    }
    struct stat buf;
    int ret = fstat(m_fd, &buf);
    if(-1 == ret){
        cout << "stat " << m_file.c_str() << "failed errno:" << errno << endl;
        return -1;
    }
    m_size = buf.st_size;

    m_mmap_base = mmap(NULL, m_size-m_pos, PROT_READ, MAP_SHARED, m_fd, m_pos);
    if(m_mmap_base == MAP_FAILED){
        cout << "mmap " << m_file.c_str() << "failed errno:" << errno << endl;
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
    if(off < m_pos || off > m_size){
        cout << "read para error off: " << off << " pos:" << m_pos 
             << " size:" << m_size << endl;
        return 0;
    }
    
    size_t copy_size = std::min(count, m_size-off);
    memcpy(buf, (char*)m_mmap_base+off-m_pos, copy_size);

    return copy_size;
}
    
int MmapReadFile::hashcode()
{
    //todo: how to geneate hashcode of each file
    return fid;
}
