#include "../log/log.h"
#include "seq_generator.hpp"

IoVersion::IoVersion(uint32_t fid, uint64_t ioid)
{
    m_fileid = fid;
    m_ioseq  = ioid;
}

IoVersion::IoVersion(const IoVersion& other)
{
    m_fileid = other.m_fileid;
    m_ioseq  = other.m_ioseq;
}

IoVersion& IoVersion::operator=(const IoVersion& other)
{
    if(this != &other){
        m_fileid = other.m_fileid;
        m_ioseq  = other.m_ioseq;
    }
    return *this;
}

bool operator<(const IoVersion& a, const IoVersion& b)
{
    if(a.m_fileid !=b.m_fileid){
        return a.m_fileid < b.m_fileid; 
    } else {
        if(a.m_ioseq < b.m_ioseq){
            return a.m_ioseq < b.m_ioseq;
        }
    }
    return false;
}

ostream& operator<<(ostream& out, const IoVersion& b)
{
   out << "[fileid: " << b.m_fileid << " ioseq:" << b.m_ioseq << "]";
   return out;
}

uint32_t IDGenerator::s_volume_idx = 0;

void IDGenerator::add_file(string jfile)
{
    auto it = m_journal_files.find(jfile);
    if(it != m_journal_files.end()){
        LOG_ERROR << "IDGenerator add " << jfile << "failed existed";
        return;
    }
    uint64_t fid = (s_volume_idx << 30) | (m_journal_files_num);
    m_journal_files.emplace(pair<string, IoVersion*>(jfile, new IoVersion(fid, 0)));
    m_journal_files_num++;
}

void IDGenerator::del_file(string jfile)
{
    auto it = m_journal_files.find(jfile);
    if(it != m_journal_files.end()){
        m_journal_files.erase(it); 
    }
} 

IoVersion IDGenerator::get_version(string file)
{
    auto it = m_journal_files.find(file);
    if(it == m_journal_files.end()){
        LOG_ERROR<< "IIDGenerator get_id failed";
        return IoVersion(-1,-1);
    }

    IoVersion v = *(it->second);
    it->second->m_ioseq++;
    return v;
}
