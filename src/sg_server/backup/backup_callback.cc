#include <iterator>
#include <iostream>
#include "backup_callback.h"
#include "log.h"

void BackupCallback::join_group_callback(const list<string> &old_buckets, const list<string> &new_buckets)
{
    //TODO do backup change group action
    LOG_DEBUG << "join_group_callback, old buckets size:" << old_buckets.size();
    LOG_DEBUG << "join_group_callback, new buckets size:" << new_buckets.size();

}

void BackupCallback::leave_group_callback(const list<string> &old_buckets, const list<string> &new_buckets)
{
    //TODO do backup change group action
    LOG_DEBUG << "leave_group_back, old buckets size:" << old_buckets.size();
    LOG_DEBUG << "leave_group_back, new buckets size:" << new_buckets.size();
}