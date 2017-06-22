#ifndef SRC_COMMON_UTILS_H_
#define SRC_COMMON_UTILS_H_
#include <stdbool.h>
#include <cpuid.h>
#include <string>

bool is_support_sse4_2();

void memory_barrier();

std::string backup_to_snap_name(std::string backup_name);
std::string snap_to_backup_name(std::string snap_name);

// mapings between replicate operate uuid and snapshot name
std::string snap_name_to_operate_uuid(const std::string& snap_name);
std::string operate_uuid_to_snap_name(const std::string& operate_id);

std::string rpc_address(const std::string& host, const uint16_t& port);

void save_file(const std::string& fname, const char* buf, const size_t& len);

#endif  // SRC_COMMON_UTILS_H_
