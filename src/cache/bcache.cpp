#include "bcache.h"

bool operator<(const Bkey& a, const Bkey& b)
{
    if(a.m_off != b.m_off){
        return a.m_off < b.m_off;
    } else {
        if(a.m_len != b.m_len){
            return a.m_len < b.m_len;
        } else {
            if(a.m_seq != b.m_seq)
                return a.m_seq < b.m_seq;
        }
    }

    return true;
}

int Bcache::read(off_t off, size_t len, char* buf)
{
    //todo
    return 0;
}

void Bcache::trace()
{
    cout << "-----bcache-------" << endl;
    for(auto it : m_bcache)
    {
        cout <<"(" << it.first.m_off << " "  \
                   << it.first.m_len << " "  \
                   << it.first.m_seq << ")"  \
             <<"[" << it.second.use_count() << "]" << endl;
    }
}
