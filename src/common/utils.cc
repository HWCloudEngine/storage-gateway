#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netinet/in.h>
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

std::string rpc_address(const std::string& host, const uint16_t& port) {
    return host + ":" + std::to_string(port);
}

#if defined(__GNUC__) && defined(__x86_64__)
typedef unsigned uint128_t __attribute__((mode(TI)));
bool mem_is_zero(const char* data, size_t len) {
    if (len / sizeof(uint128_t) > 0) {
        while (((unsigned long long)data) & 15) {
            if (*(uint8_t*)data != 0) {
                return false;
            }
            data += sizeof(uint8_t);
            --len;
        }
        const char* data_start = data;
        const char* max128 = data + (len/sizeof(uint128_t)) * sizeof(uint128_t);
        while (data < max128) {
            if (*(uint128_t*)data != 0) {
                return false;
            }
            data += sizeof(uint128_t);
        }
        len -= (data-data_start);
    }

    const char* max = data + len;
    const char* max32 = data + (len/sizeof(uint32_t)) * sizeof(uint32_t);
    while (data < max32) {
        if (*(uint32_t*)data != 0) {
            return false;
        } 
        data += sizeof(uint32_t);
    }

    while (data < max) {
        if (*(uint8_t*)data != 0) {
            return false;
        }
        data += sizeof(uint8_t);
    }
    return true;
}
#else
bool mem_is_zero(const char* data, size_t len) {
    const char* end = data + len;
    while (data < end) {
        if (*data != 0) {
            return false;
        }
        ++data;
    }
    return true;
}
#endif

bool network_reachable(const char* ip_addr, short port) {
    struct sockaddr_in server_addr;
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock < 0) {
        return false;
    }
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip_addr);
    server_addr.sin_port = htons(port);
    int ret = connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (ret < 0) {
        return false;
    }
    close(sock);
    return true;
}
