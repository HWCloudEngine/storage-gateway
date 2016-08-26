#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>
#include "cache_recover.hpp"

int main(int argc, char** argv)
{
    CacheRecovery* cr = new CacheRecovery();
    cr->start();
    cr->stop();
    pause();
    return 0;
}
