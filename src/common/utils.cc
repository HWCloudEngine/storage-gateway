#include <string.h>
#include "utils.h"

std::string rpc_server_address(const std::string& host, const int16_t& port) {
    std::string addr;
    addr.append(host);
    addr.append(":");
    addr.append(std::to_string(port));
    return addr;
}

bool is_support_sse4_2() {
    unsigned int eax, ebx, ecx, edx;
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    if (ecx & bit_SSE4_2)
        return true;
    return false;
}

const char* SUFFIX = ".snapshot";

std::string backup_to_snap_name(std::string backup_name) {
    return backup_name.append(SUFFIX);
}

std::string snap_to_backup_name(std::string snap_name) {
    size_t pos = snap_name.find(SUFFIX);
    if (pos == std::string::npos) {
        return "";
    }
    return snap_name.erase(pos, strlen(SUFFIX));
}

// generate  snap_name by operate uuid
std::string operate_uuid_to_snap_name(const std::string& operate_id) {
    return operate_id;
}

// extract operate uuid from snap_name
std::string snap_name_to_operate_uuid(const std::string& snap_name) {
    return snap_name;
}

