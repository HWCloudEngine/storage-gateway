#include <sys/uio.h>
#include <assert.h>
#include "../common/crc32.h"
#include "../log/log.h"
#include "journal_entry.h"
#include "../rpc/message.pb.h"
using huawei::proto::WriteMessage;
using huawei::proto::SnapshotMessage;

JournalEntry::JournalEntry()
{
}

JournalEntry::JournalEntry(uint64_t& io_handle, 
                           uint64_t& seq, 
                           journal_event_type_t& type, 
                           shared_ptr<Message>&  message)
{
    this->handle.push_back(io_handle);
    this->sequence = seq;
    this->type = type;
    this->length = 0;
    this->message = message;
    this->crc = 0;
    this->message_serialized_data.clear();
}

JournalEntry::JournalEntry(const JournalEntry& other)
{
    this->handle = other.handle;
    this->sequence = other.sequence;
    this->type  = other.type;
    this->length = other.length;
    this->message = other.message;
    this->crc = other.crc;
    this->message_serialized_data.clear();
}

JournalEntry& JournalEntry::operator=(const JournalEntry& other)
{
    this->handle = other.handle;
    this->sequence = other.sequence;
    this->type  = other.type;
    this->length = other.length;
    this->message = other.message;
    this->crc = other.crc;
    this->message_serialized_data = other.message_serialized_data;
    return *this;
}

JournalEntry::~JournalEntry()
{
    /*todo free resource */
}

void JournalEntry::set_handle(uint64_t io_handle)
{
    handle.push_back(io_handle);
}

vector<uint64_t> JournalEntry::get_handle()const
{
    return handle;
}

void JournalEntry::set_sequence(uint64_t seq)
{
    sequence = seq;
}

uint64_t JournalEntry::get_sequence()const
{
    return sequence;
}

void JournalEntry::set_type(journal_event_type_t type)
{
    this->type = type;
}

journal_event_type_t JournalEntry::get_type()const
{
    return (journal_event_type_t)type;
}

void JournalEntry::set_length(uint32_t len)
{
    this->length = len;
}

uint32_t JournalEntry::get_length()const
{
    return length;
}

void JournalEntry::set_message(shared_ptr<Message> message)
{
    this->message = message;
}

shared_ptr<Message> JournalEntry::get_message()const
{
    return message;
}

void JournalEntry::set_crc(uint32_t crc)
{
    this->crc = crc;
}

uint32_t JournalEntry::get_crc()const
{
    return crc;
}

uint32_t JournalEntry::calculate_crc()
{
    uint32_t initial = 0;
    assert(!message_serialized_data.empty());
    this->crc = crc32c(message_serialized_data.c_str(), 
                       message_serialized_data.size(), 
                       initial);
    return this->crc;
}

size_t JournalEntry::persist(int fd, off_t off)
{
    if(message_serialized_data.empty()){
        serialize();
        calculate_crc();
    }

    struct iovec iov[4];
    iov[0].iov_base = &type;
    iov[0].iov_len  = sizeof(type);
    iov[1].iov_base = &length;
    iov[1].iov_len  = sizeof(length);
    iov[2].iov_base = (void*)message_serialized_data.c_str();
    iov[2].iov_len  = length;
    iov[3].iov_base = &crc;
    iov[3].iov_len  = sizeof(crc);
    
    size_t write_size = sizeof(type)+sizeof(length) + length +sizeof(crc);
    size_t write_ret  = pwritev(fd, iov, 4, off);
    assert(write_size == write_size);
    
    //LOG_INFO << "persist type:" << type << " length:" << length 
    //         << " crc:" << crc << " ok";
    return write_ret;
}

size_t JournalEntry::get_persit_size()const
{
    return sizeof(type) + sizeof(length) + length + sizeof(crc);
}

size_t JournalEntry::persist(FILE* file, off_t off)
{
    if(message_serialized_data.empty()){
        serialize();
        calculate_crc();
    }

    size_t buf_len = sizeof(type) + sizeof(length) + length + sizeof(crc) ;
    char*  buf = (char*)malloc(buf_len);
    char*  write_buf = buf;    
    memcpy(write_buf, (char*)&type, sizeof(type));
    write_buf += sizeof(type);
    memcpy(write_buf, (char*)&length, sizeof(length));
    write_buf += sizeof(length);
    memcpy(write_buf, (char*)message_serialized_data.c_str(), length);
    write_buf += length;
    memcpy(write_buf, (char*)&crc, sizeof(crc));
    
    int ret = fseek(file, off, SEEK_SET);
    ret = fwrite(buf, 1, buf_len, file);
    assert(ret == buf_len);
    free(buf);
    
    fflush(file);

    //LOG_INFO << "persist type:" << type << " length:" << length 
    //         << " crc:" << crc  << " ok";
    return buf_len;
}

size_t JournalEntry::copy_entry(string& buffer){
    buffer.append((char*)&type, sizeof(type));
    buffer.append((char*)&length, sizeof(length));
    buffer.append((char*)message_serialized_data.c_str(), length);
    buffer.append((char*)&crc, sizeof(crc));
    size_t size = sizeof(type) + sizeof(length) + length + sizeof(crc);
    return size;
}

ssize_t JournalEntry::parse(int fd, size_t fsize, off_t off)
{
    off_t start = off;
    
    /*read type and length*/
    struct iovec iov[2];
    iov[0].iov_base = &type;
    iov[0].iov_len  = sizeof(type);
    iov[1].iov_base = &length;
    iov[1].iov_len  = sizeof(length);
    size_t read_ret = preadv(fd, iov, 2, start);
    if(read_ret != (sizeof(type) + sizeof(length))){
        LOG_ERROR << "parse type and length failed";
        return -1;
    }
    if(type != IO_WRITE && type != SNAPSHOT_CREATE && type != SNAPSHOT_DELETE &&
       type != SNAPSHOT_ROLLBACK){
        LOG_ERROR << "parse type failed";
        return -1; 
    }
    if((off + length) > fsize){
        LOG_ERROR << "parse length over region failed";
        return -1;
    }
    start += read_ret;

    /*for store serialize data*/
    char* data = (char*)malloc(length);
    assert(data != nullptr);

    /*read data and crc*/
    struct iovec iov1[2];
    iov1[0].iov_base = data;
    iov1[0].iov_len  = length;
    iov1[1].iov_base = &crc;
    iov1[1].iov_len  = sizeof(crc);
    read_ret = preadv(fd, iov1, 2, start);
    if(read_ret != (length + sizeof(crc))){
        LOG_ERROR << "parse data and crc failed";
        return -1;
    }
    start += read_ret;

    /*message parse*/
    switch(type)
    {
    case IO_WRITE:
        message = make_shared<WriteMessage>(); 
        break;
    case SNAPSHOT_CREATE:
    case SNAPSHOT_DELETE:
    case SNAPSHOT_ROLLBACK:
        message = make_shared<SnapshotMessage>(); 
        break;
    default:
        assert(0);
        break;
    }

    message->ParseFromArray(data, length);

    /*check crc */
    uint32_t initial = 0;
    uint32_t crc_value = crc32c(data, length, initial);

    assert(crc_value == crc);

    free(data);

    //LOG_INFO << "parse type:" << type << " length:" << length 
    //         << " crc:" << crc << " crc_value:" << crc_value << " ok";
    return start;
}

/*todo: optimize*/
ssize_t JournalEntry::parse(FILE* file, off_t off)
{
    off_t start = off;
    /*read type*/
    fseek(file, start, SEEK_SET); 
    size_t ret = fread((char*)&type, 1, sizeof(type), file);
    assert(ret == sizeof(type));
    start += sizeof(type);
    
    /*read length*/
    fseek(file, start, SEEK_SET); 
    ret = fread((char*)&length, 1, sizeof(length), file);
    assert(ret == sizeof(length));
    start += sizeof(length);
    
    /*read data*/
    char* data = (char*)malloc(length);
    assert(data != nullptr);
    fseek(file, start, SEEK_SET); 
    ret = fread(data, 1, length, file);
    assert(ret == length);
    start += length;
    
    /*read crc*/
    fseek(file, start, SEEK_SET); 
    ret = fread((char*)&crc, 1, sizeof(crc), file);
    assert(ret == sizeof(crc));
    start += sizeof(crc);

    /*message parse*/
    switch(type)
    {
    case IO_WRITE:
        message = make_shared<WriteMessage>(); 
        break;
    case SNAPSHOT_CREATE:
    case SNAPSHOT_DELETE:
    case SNAPSHOT_ROLLBACK:
        message = make_shared<SnapshotMessage>(); 
        break;
    default:
        assert(0);
        break;
    }

    message->ParseFromArray(data, length);

    /*check crc */
    uint32_t initial = 0;
    uint32_t crc_value = crc32c(data, length, initial);
    assert(crc_value == crc);
    
    free(data);

    //LOG_INFO << "parse type:" << type << " length:" << length 
    //         << " crc:" << crc << " crc_value:" << crc_value << " ok";
    return start;
}

bool JournalEntry::serialize()
{
    message->SerializeToString(&message_serialized_data);
    this->length = message_serialized_data.size();
    return true;
}

bool JournalEntry::deserialize(const string& in)
{
    message->ParseFromString(in);
    return true;
}

void JournalEntry::clear_serialized_data()
{
    message_serialized_data.clear();
}

ostream& operator<<(ostream& cout, const JournalEntry& entry)
{
    LOG_INFO << " seq:"  << entry.get_sequence()
             << " type:" << entry.get_type()
             << " len:"  << entry.get_length()
             << " crc:"  << entry.get_crc();
    return cout;
}
