#ifndef SRC_COMMON_UTILS_H_
#define SRC_COMMON_UTILS_H_
#include <stdbool.h>
#include <cpuid.h>
#include <string>

bool is_support_sse4_2();

std::string rpc_server_address(const std::string& host, const int16_t& port);

std::string backup_to_snap_name(std::string backup_name);
std::string snap_to_backup_name(std::string snap_name);

// mapings between replicate operate uuid and snapshot name
std::string snap_name_to_operate_uuid(const std::string& snap_name);
std::string operate_uuid_to_snap_name(const std::string& operate_id);

#endif  // SRC_COMMON_UTILS_H_
