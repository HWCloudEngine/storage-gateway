#include "utils.h"

bool is_support_sse4_2()
{
    unsigned int eax, ebx, ecx, edx;
    __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    if (ecx & bit_SSE4_2)
        return true;
    return false;
}

