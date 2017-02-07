#ifndef __BACKUP_UTIL_H_
#define __BACKUP_UTIL_H_
#include <string>
using namespace std;

string backup_to_snap_name(string backup_name);
string snap_to_backup_name(string snap_name);

#endif
