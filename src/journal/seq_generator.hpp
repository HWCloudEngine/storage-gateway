#ifndef __SEQ_GENERATOR_H
#define __SEQ_GENERATOR_H

#include <unistd.h>
#include <iostream>
#include <string>
#include <map>

using namespace std;

//todo: how to recycle io sequence

//id sequence 
class IoVersion
{
public:
    IoVersion() = default;

    IoVersion(uint32_t fid, uint64_t ioid){
        m_fileid = fid;
        m_ioseq  = ioid;
    }
    IoVersion(const IoVersion& other){
        m_fileid = other.m_fileid;
        m_ioseq  = other.m_ioseq;
    }
    IoVersion& operator=(const IoVersion& other){
        if(this != &other){
            m_fileid = other.m_fileid;
            m_ioseq  = other.m_ioseq;
        }
        return *this;
    }
    ~IoVersion(){}

    friend bool operator<(const IoVersion& a, const IoVersion& b);
    friend ostream& operator<<(ostream& out, const IoVersion& b);

    uint64_t  m_fileid;  //journal file
    uint64_t  m_ioseq;   //entry sequence in journal file
};

//each volume own a id generator 
class IDGenerator
{
    /*each volume index*/
    static uint32_t s_volume_idx;
public:
    IDGenerator(){s_volume_idx++;}
    ~IDGenerator(){} 
    
    void add_file(string jfile);
    void del_file(string jfile);

    IoVersion get_version(string file);
    
private:
    string                  m_affiliate_volume;
    uint32_t                m_journal_files_num;
    map<string, IoVersion*> m_journal_files;
};

#endif
