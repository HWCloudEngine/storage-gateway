#ifndef _SIMPLE_CALLBACK_H
#define _SIMPLE_CALLBACK_H
#include "callback_interface.h"
class SimpleCallback: public ICallback
{
public:
    SimpleCallback(){}
    ~SimpleCallback(){}
    virtual void join_group_callback(const list<string> &old_buckets, const list<string> &new_buckets);
    virtual void leave_group_callback(const list<string> &old_buckets, const list<string> &new_buckets);
};
#endif

