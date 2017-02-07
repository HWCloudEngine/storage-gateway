#ifndef _UTILS_H
#define _UTILS_H
#include <stdbool.h>
#include <cpuid.h>
#include <string>

bool is_support_sse4_2();

std::string backup_to_snap_name(std::string backup_name);
std::string snap_to_backup_name(std::string snap_name);

#endif
