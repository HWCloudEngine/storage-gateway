#include "backup_util.h"

const string SUFFIX = ".snapshot";

string backup_to_snap_name(string backup_name)
{
    return backup_name.append(SUFFIX);
}

string snap_to_backup_name(string snap_name)
{
    size_t pos = snap_name.find(SUFFIX);
    if(pos == string::npos){
        return nullptr; 
    }
    
    return snap_name.erase(pos, SUFFIX.length());
}


