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
#include "../../rpc/message.pb.h"

using google::protobuf::Message;
using huawei::proto::WriteMessage;

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
               shared_ptr<JournalEntry> entry)
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
    size_t size = 0;
    if(cache_type == IN_MEM){
        if(IO_WRITE == journal_entry->get_type()){
            shared_ptr<Message> message = journal_entry->get_message();
            shared_ptr<WriteMessage> write_message = dynamic_pointer_cast
                                                     <WriteMessage>(message);  
            size += write_message->data().size();
        } else {
            /*other message*/
        }
    } else {
        ; 
    }
    return size;
}

int File::fid = 0;

File::File(string file, off_t start_pos, bool eos)
    :m_file(file), m_start_pos(start_pos), m_end_pos(UINT_MAX), m_eos(eos)
{
    fid++;
}

File::File(string file, off_t start_pos, off_t end_pos, bool eos)
    :m_file(file), m_start_pos(start_pos), m_end_pos(end_pos), m_eos(eos)
{
    fid++;
}

ssize_t File::read_entry(off_t off, shared_ptr<JournalEntry>& entry)
{
    entry = make_shared<JournalEntry>();
    return entry->parse(m_fd, off);
}

int File::open()
{
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
    LOG_INFO << "open file:" << m_file << " size:" << m_size;
    return m_fd;
}

void File::close()
{
    if(-1 != m_fd){
        ::close(m_fd);
    } 
}

int File::hashcode()
{
    //todo: how to geneate hashcode of each file
    return fid;
}
