#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>
#include "../common/log_header.h"
#include "proxy.h"

using namespace std;

struct log_header_deleter
{
    void operator()(log_header_t* p){
        cout << "log header deleter called " << endl;
        free(p);
    }
};

int main(int argc, char** argv)
{
    string blk = "./blk";
    Cproxy proxy(blk);

    string   log_file = "./log";

    uint64_t  log_seq  = 0;
    off_t     log_off  = 0;
    off_len_t i1 = {0, 100};
    size_t lsize = sizeof(log_header_t) + sizeof(off_len_t) + i1.length;
    log_header_t* log_1 = (log_header_t*)malloc(lsize);
    log_1->count = 1;
    memcpy((char*)log_1->off_len, &i1, sizeof(off_len_t));
    char* d1 = (char*)malloc(100);
    memcpy((char*)log_1+sizeof(log_header_t)+sizeof(off_len_t), d1, i1.length);
    shared_ptr<log_header_t> le1(log_1, log_header_deleter());
    proxy.write(log_seq, log_file, log_off, le1);

    log_seq++;
    log_off += lsize;
    off_len_t i2 = {100, 100};
    lsize = sizeof(log_header_t) + sizeof(off_len_t) + i2.length;
    log_header_t* log_2 = (log_header_t*)malloc(lsize);
    log_2->count = 1;
    memcpy((char*)log_2->off_len, &i2, sizeof(off_len_t));
    char* d2 = (char*)malloc(100);
    memcpy((char*)log_2+sizeof(log_header_t) + sizeof(off_len_t), d2, i2.length);
    shared_ptr<log_header_t> le2(log_2, log_header_deleter());
    proxy.write(log_seq, log_file, log_off, le2);


    log_seq++;
    log_off += lsize;
    off_len_t i3 = {200, 100};
    lsize = sizeof(log_header_t) + sizeof(off_len_t) + i3.length;
    log_header_t* log_3 = (log_header_t*)malloc(lsize);
    log_3->count = 1;
    memcpy((char*)log_3->off_len, &i3, sizeof(off_len_t));
    char* d3 = (char*)malloc(100);
    memcpy((char*)log_3+sizeof(log_header_t) + sizeof(off_len_t), d3, i3.length);
    shared_ptr<log_header_t> le3(log_3, log_header_deleter());
    proxy.write(log_seq, log_file, log_off, le3);

    proxy.trace();

    pause();

    return 0;
}
