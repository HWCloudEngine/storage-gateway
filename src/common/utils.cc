#include <fstream>
#include "utils.h"

bool is_support_sse4_2() {
    unsigned int eax, ebx, ecx, edx;
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    if (ecx & bit_SSE4_2) {
        return true;
    }
    return false;
}

void memory_barrier() {
    __asm__ __volatile__("" : : : "memory");
}

const std::string SUFFIX = ".snapshot";

std::string backup_to_snap_name(std::string backup_name) {
    return backup_name.append(SUFFIX);
}

std::string snap_to_backup_name(std::string snap_name) {
    size_t pos = snap_name.find(SUFFIX);
    if (pos == std::string::npos) {
        return "";
    }
    return snap_name.erase(pos, SUFFIX.length());
}

// generate  snap_name by operate uuid
std::string operate_uuid_to_snap_name(const std::string& operate_id) {
    return operate_id;
}

// extract operate uuid from snap_name
std::string snap_name_to_operate_uuid(const std::string& snap_name) {
    return snap_name;
}

void save_file(const std::string& fname, const char* buf, const size_t& len) {
    std::ofstream outfile(fname, std::ofstream::binary);
    outfile.write(buf, len);
    outfile.close();
}

std::string rpc_address(const std::string host, const uint16_t& port) {
    return host + ":" + std::to_string(port);
}
