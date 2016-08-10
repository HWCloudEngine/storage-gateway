#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>
#include "../message.hpp"
#include "../seq_generator.hpp"
#include "../../common/log_header.h"
#include "cache_proxy.h"

using namespace std;
using namespace Journal;

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
    CacheProxy proxy(blk);

    string   log_file = "./log";

    IDGenerator id_maker;    
    id_maker.add_file(log_file);

    IoVersion log_seq  = id_maker.get_version(log_file);
    off_t     log_off  = 0;
    off_len_t i1  = {10, 10};
    size_t lsize  = sizeof(log_header_t) + sizeof(off_len_t) + i1.length;
    log_header_t* log_1 = (log_header_t*)malloc(lsize);
    log_1->count = 1;
    memcpy((char*)log_1->off_len, &i1, sizeof(off_len_t));
    char* d1 = (char*)malloc(i1.length);
    memcpy((char*)log_1+sizeof(log_header_t)+sizeof(off_len_t), d1, i1.length);
    //shared_ptr<log_header_t> le1(log_1, log_header_deleter());
    shared_ptr<ReplayEntry> le1(new ReplayEntry((char*)log_1, lsize, log_seq));
    proxy.write(log_seq, log_file, log_off, le1);

    //log_seq  = id_maker.get_version(log_file);
    //log_off += lsize;
    //off_len_t i2 = {30, 10};
    //lsize = sizeof(log_header_t) + sizeof(off_len_t) + i2.length;
    //log_header_t* log_2 = (log_header_t*)malloc(lsize);
    //log_2->count = 1;
    //memcpy((char*)log_2->off_len, &i2, sizeof(off_len_t));
    //char* d2 = (char*)malloc(i2.length);
    //memcpy((char*)log_2+sizeof(log_header_t) + sizeof(off_len_t), d2, i2.length);
    ////shared_ptr<log_header_t> le2(log_2, log_header_deleter());
    //shared_ptr<ReplayEntry> le2(new ReplayEntry((char*)log_2, lsize, log_seq));
    //proxy.write(log_seq, log_file, log_off, le2);

    //log_seq  = id_maker.get_version(log_file);
    //log_off += lsize;
    //off_len_t i3 = {15, 10};
    //lsize = sizeof(log_header_t) + sizeof(off_len_t) + i3.length;
    //log_header_t* log_3 = (log_header_t*)malloc(lsize);
    //log_3->count = 1;
    //memcpy((char*)log_3->off_len, &i3, sizeof(off_len_t));
    //char* d3 = (char*)malloc(i3.length);
    //memcpy((char*)log_3+sizeof(log_header_t) + sizeof(off_len_t), d3, i3.length);
    ////shared_ptr<log_header_t> le3(log_3, log_header_deleter());
    //shared_ptr<ReplayEntry> le3(new ReplayEntry((char*)log_3, lsize, log_seq));
    //proxy.write(log_seq, log_file, log_off, le3);

    //log_seq  = id_maker.get_version(log_file);
    //log_off += lsize;
    //off_len_t i4 = {5, 15};
    //lsize = sizeof(log_header_t) + sizeof(off_len_t) + i4.length;
    //log_header_t* log_4 = (log_header_t*)malloc(lsize);
    //log_4->count = 1;
    //memcpy((char*)log_4->off_len, &i4, sizeof(off_len_t));
    //char* d4 = (char*)malloc(i4.length);
    //memcpy((char*)log_4+sizeof(log_header_t) + sizeof(off_len_t), d4, i4.length);
    ////shared_ptr<log_header_t> le4(log_4, log_header_deleter());
    //shared_ptr<ReplayEntry> le4(new ReplayEntry((char*)log_4, lsize, log_seq));
    //proxy.write(log_seq, log_file, log_off, le4);
   
    //cout << "test get" << endl;
    //Bcache* bcache = proxy.get_bcache();
    //Bkey k(10, 10, 0);
    //shared_ptr<CEntry> v;
    //bcache->get(k, v);

    off_t  r_o = 15;
    size_t r_l = 10;
    char*  r_b = (char*)malloc(r_l);
    proxy.read(r_o, r_l, r_b);

    //proxy.trace();
    //Jkey top = proxy.top();
    //shared_ptr<CEntry> entry = proxy.retrieve(top);
    //proxy.reclaim(entry);
    //proxy.trace();

    pause();

    return 0;
}
