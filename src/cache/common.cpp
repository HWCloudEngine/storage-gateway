#include <utility>
#include "common.h"

CEntry::CEntry(uint64_t seq, string file, off_t offset)
{
    log_seq    = seq;
    log_file   = file;
    log_offset = offset;
    cache_type = IN_LOG;
    log_entry  = nullptr;
}

CEntry::CEntry(uint64_t seq, string file, off_t offset, shared_ptr<ReplayEntry> entry)
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

