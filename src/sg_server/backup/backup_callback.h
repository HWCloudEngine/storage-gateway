#ifndef _BACKUP_CALLBACK_H
#define _BACKUP_CALLBACK_H
#include "callback_interface.h"
class BackupCallback: public ICallback
{
public:
    BackupCallback(){}
    ~BackupCallback(){}
    virtual void join_group_callback(const list<string> &old_buckets, const list<string> &new_buckets);
    virtual void leave_group_callback(const list<string> &old_buckets, const list<string> &new_buckets);
};
#endif

