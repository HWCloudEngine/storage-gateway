/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    rep_task.h
* Author: 
* Date:         2017/02/16
* Version:      1.0
* Description:
* 
************************************************/
#ifndef REP_TASK_H_
#define REP_TASK_H_
#include <fstream>
#include <string>
#include <vector>
#include "rep_type.h"
#include "sg_server/transfer/transfer_task.h"
#include "../snap_client_wrapper.h"
class RepContext:public TaskContext{
protected:
    std::string vol_id;
    int64_t j_counter; // (start at)journal counter
    uint64_t end_off; // end offset of journal file
    bool is_open; // journal file is opened?
    std::function<void(std::shared_ptr<TransferTask>&)> callback;
public:
    RepContext(
            const std::string& _vol_id,
            const int64_t& _j_counter,
            const uint64_t& _end_off,
            const bool& _is_open,
            std::function<void(std::shared_ptr<TransferTask>&)> _callback):
            vol_id(_vol_id),
            j_counter(_j_counter),
            end_off(_end_off),
            is_open(_is_open),
            callback(_callback){}

    RepContext(){}
    ~RepContext(){}

    std::string& get_vol_id(){
        return vol_id;
    }
    void set_vol_id(const std::string& _vol_id){
        vol_id = _vol_id;
    }

    int64_t& get_j_counter(){
        return j_counter;
    }
    void set_j_counter(const int64_t& _counter){
        j_counter = _counter;
    }

    uint64_t get_end_off(){
        return end_off;
    }
    void set_end_off(const uint64_t& _end){
        end_off = _end;
    }

    bool& get_is_open(){
        return is_open;
    }
    void set_j_counter(const bool& _is_open){
        is_open = _is_open;
    }

    std::function<void(std::shared_ptr<TransferTask>&)>& get_callback(){
        return callback;
    }
    void set_callback(std::function<void(std::shared_ptr<TransferTask>&)> _callback){
        callback = _callback;
    }
};

class JournalTask:public TransferTask {
private:
    std::string path;
    uint64_t start_off;
    std::shared_ptr<RepContext> ctx;

    // internal params
    bool end; // task done, ReplicateEndReq was sent
    uint64_t cur_off;
    std::ifstream is;
    char* buffer;
    uint64_t package_id;
public:
    JournalTask(const uint64_t& _start,
            const std::string& _path,
            std::shared_ptr<RepContext> _context):
            start_off(_start),
            cur_off(_start),
            end(false),
            buffer(nullptr),
            path(_path),
            ctx(_context),
            package_id(0),
            TransferTask(_context){
        init();
    }

    ~JournalTask(){
        if(is.is_open())
            is.close();
        if(buffer)
            free(buffer);
    }

    void set_path(const string& _path);
    string& get_path();

    void set_start_off(const uint64_t& _start);
    uint64_t get_start_off();

    bool has_next_package() override;

    TransferRequest* get_next_package() override;

    int reset() override;

    int init();
};

// TODO:split to multiple journal files of SnapTask
class DiffSnapTask:public TransferTask {
private:
    std::string pre_snap;
    std::string cur_snap;
    std::shared_ptr<RepContext> ctx;

    // internal params
    bool end; // ReplicateEndReq was constructed, sending
    bool all_data_sent; // all data was sent
    std::vector<DiffBlocks> diff_blocks;
    int vector_cursor; // cursor of DiffBlocks vector
    int array_cursor; // cursor of array in DiffBlock
    uint64_t cur_off; // pair with j_counter, indicate whether there was space in journal
    int64_t sub_counter; // sub journal counter, start from 1
    uint64_t max_journal_size;
    char* buffer;
    uint64_t package_id;

public:
    DiffSnapTask(const std::string& _pre,
            const std::string& _cur,
            std::shared_ptr<RepContext> _context):
            pre_snap(_pre),
            cur_snap(_cur),
            ctx(_context),
            TransferTask(_context),
            package_id(0),
            end(false){
        init();
    }
    ~DiffSnapTask(){
        if(buffer){
            free(buffer);
        }
    }

    std::string& get_pre_snap();
    void set_pre_snap(const std::string& _snap);

    std::string& get_cur_snap();
    void set_cur_snap(const std::string& _snap);

    bool has_next_package() override;

    TransferRequest* get_next_package() override;

    int reset() override;

    int init();
};

class BaseSnapTask:public TransferTask {
private:
    std::string base_snap;
    uint64_t vol_size;

    // internal memebers
    bool end;
    uint64_t package_id;
    std::shared_ptr<RepContext> ctx;
    uint64_t read_off; // where to read from
    char* buffer;
    uint64_t cur_off; // pair with j_counter, indicate whether there was space in journal
    int64_t sub_counter; // sub journal counter, start from 1
    uint64_t max_journal_size;

public:
    BaseSnapTask(const std::string& _base,
                      const uint64_t& _vol_size,
                      std::shared_ptr<RepContext> _context):
            base_snap(_base),
            vol_size(_vol_size),
            TransferTask(_context),
            package_id(0),
            cur_off(0),
            ctx(_context),
            end(false){
        init();
    }

    ~BaseSnapTask(){
        if(buffer){
            free(buffer);
        }
    }

    std::string& get_base_snap();
    void set_base_snap(const std::string& _snap);

    uint64_t get_vol_size();
    void set_vol_size(const uint64_t& _size);

    bool has_next_package() override;

    TransferRequest* get_next_package() override;

    int reset() override;

    int init();
};
#endif