#ifndef __SEQ_GENERATOR_H
#define __SEQ_GENERATOR_H

#include <unistd.h>
#include <iostream>
#include <string>
#include <map>
#include <mutex>

using namespace std;

/*todo: how to recycle io sequence*/

/*id sequence*/
class IoVersion
{
public:
    IoVersion() = default;

    IoVersion(uint32_t fid, uint64_t ioid);
    IoVersion(const IoVersion& other);
    IoVersion& operator=(const IoVersion& other);
    ~IoVersion(){}

    friend bool operator<(const IoVersion& a, const IoVersion& b);
    friend ostream& operator<<(ostream& out, const IoVersion& b);
    
    /*journal file unique id*/
    uint64_t  m_fileid;
    /*journal entry unique id in file*/
    uint64_t  m_ioseq;
};

/*each volume own a id generator */
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
    mutex m_lock;
    uint32_t m_journal_files_num;
    map<string, IoVersion*> m_journal_files;
};

#endif
