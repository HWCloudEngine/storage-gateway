#include "utils.h"

bool is_support_sse4_2()
{
    unsigned int eax, ebx, ecx, edx;
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    if (ecx & bit_SSE4_2)
        return true;
    return false;
}

const std::string SUFFIX = ".snapshot";

std::string backup_to_snap_name(std::string backup_name)
{
    return backup_name.append(SUFFIX);
}

std::string snap_to_backup_name(std::string snap_name)
{
    size_t pos = snap_name.find(SUFFIX);
    if(pos == std::string::npos){
        return ""; 
    }
    
    return snap_name.erase(pos, SUFFIX.length());
}
