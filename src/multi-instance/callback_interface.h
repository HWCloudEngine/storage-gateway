#ifndef _ICALLBACK_H
#define _ICALLBACK_H
#include <string>
#include <list>
using namespace std;

class ICallback
{
public:
    public:
    ICallback(){}
    virtual ~ICallback(){}
    virtual void join_group_callback(const list<string> &old_buckets, const list<string> &new_buckets) = 0;
    virtual void leave_group_callback(const list<string> &old_buckets, const list<string> &new_buckets) = 0;
};

#endif
