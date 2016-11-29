#include "../log/log.h"
#include "journal_entry.h"

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
}

JournalEntry::JournalEntry(const JournalEntry& other)
{
    this->handle = other.handle;
    this->sequence = other.sequence;
    this->type  = other.type;
    this->length = other.length;
    this->message = other.message;
    this->crc = other.crc;
}

JournalEntry& JournalEntry::operator=(const JournalEntry& other)
{
    this->handle = other.handle;
    this->sequence = other.sequence;
    this->type  = other.type;
    this->length = other.length;
    this->message = other.message;
    this->crc = other.crc;
    return *this;
}

JournalEntry::~JournalEntry()
{

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

size_t JournalEntry::persist(int fd, off_t off)
{
    /*todo*/
    return 0;
}

size_t JournalEntry::persist(FILE* file, off_t off)
{
    /*todo*/
    return 0;
}

/*read from journal file and parse into JournalEnry*/
size_t JournalEntry::parse(int fd, off_t off)
{
    /*todo*/
    return 0;
}

size_t parse(FILE* file, off_t off)
{
    /*todo*/
    return 0;
}


/*about message*/
bool JournalEntry::serialize(ostream* output)
{
    /*todo*/
    return true;
}

bool JournalEntry::deserialize(istream* input)
{
    /*todo*/
    return true;
}

ostream& operator<<(ostream& cout, const JournalEntry& entry)
{
    LOG_INFO << " seq:"  << entry.get_sequence()
             << " type:" << entry.get_type()
             << " len:"  << entry.get_length()
             << " crc:"  << entry.get_crc();
    return cout;
}
