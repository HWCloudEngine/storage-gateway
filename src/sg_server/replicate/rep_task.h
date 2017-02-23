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
#include <cstdio>
#include <atomic>
#include <vector>
#include "rep_type.h"
#include "../snap_reader.h"
#define TASK_BLOCK_SIZE (1024*1024*1L)

typedef enum TaskStatus{
    T_UNKNOWN,
    T_WAITING,
    T_RUNNING,
    T_DONE,
    T_ERROR
}TaskStatus;

class RepTask {
protected:
    uint64_t id; // task uuid
    std::string vol_id;
    uint64_t j_counter; // (start at)journal counter
    bool is_opened; // journal file is opened?
    std::atomic<TaskStatus> status;
    uint64_t ts; // timestamp of task generation
    std::function<void(std::shared_ptr<RepTask>&)> callback;
    size_t block_size;
    uint64_t end_off; // end offset of journal file
public:
    RepTask(const uint64_t& _id,
            const std::string& _vol_id,
            const uint64_t& _j_counter,
            const bool& _is_opened,
            const TaskStatus& _status,
            const uint64_t _ts,
            std::function<void(std::shared_ptr<RepTask>&)>& _callback,
            const uint64_t& _block_size,
            const uint64_t& _end_off):
            id(_id),
            vol_id(_vol_id),
            j_counter(_j_counter),
            is_opened(_is_opened),
            status(_status),
            ts(_ts),
            callback(_callback),
            block_size(_block_size),
            end_off(_end_off){
                init();
    }
    RepTask():block_size(TASK_BLOCK_SIZE){}
    ~RepTask(){}

    bool operator<(RepTask const& task2){
        return id < task2.id;
    }

    virtual bool has_next_block() = 0;

    // get next block data, and set the offset of the journal file&set journal counter;
    // return the size of read data
    virtual int get_next_block(int64_t& counter,char* data,
                                uint64_t& off,const size_t& len)=0;
    virtual int init(){}

    size_t get_block_size(){
        return block_size;
    }
    void set_block_size(const size_t& _size){
        block_size = _size;
    }

    uint64_t get_id(){
        return id;
    }
    void set_id(const uint64_t& _id){
        id = _id;
    }

    std::string& get_vol_id(){
        return vol_id;
    }
    void set_vol_id(const std::string& _vol_id){
        vol_id = _vol_id;
    }

    uint64_t& get_j_counter(){
        return j_counter;
    }
    void set_j_counter(const uint64_t& _counter){
        j_counter = _counter;
    }

    bool get_is_opened(){
        return is_opened;
    }
    void set_is_opened(const bool& _is_opened){
        is_opened = _is_opened;
    }

    TaskStatus get_status(){
        return status.load();
    }
    void set_status(const TaskStatus& _status){
        status.store(_status);
    }

    uint64_t get_ts(){
        return ts;
    }
    void set_ts(const uint64_t _ts){
        ts = _ts;
    }

    uint64_t get_end_off(){
        return end_off;
    }
    void set_end_off(const uint64_t& _end){
        end_off = _end;
    }

    std::function<void(std::shared_ptr<RepTask>&)>& get_callback(){
        return callback;
    }
    void set_callback(std::function<void(std::shared_ptr<RepTask>&)> _callback){
        callback = _callback;
    }
};

class JournalTask:public RepTask {
private:
    std::string path;
    uint64_t start_off;
    uint64_t cur_off;
    std::ifstream is;
public:
    JournalTask(const uint64_t& _start,
            const std::string& _path):
            start_off(_start),
            cur_off(_start),
            path(_path),
            RepTask(){}
    JournalTask(const uint64_t& _start,
            const std::string& _path,
            const uint64_t& _id,
            const std::string& _vol_id,
            const uint64_t& _j_counter,
            const bool& _is_opened,
            const TaskStatus& _status,
            const uint64_t _ts,
            std::function<void(std::shared_ptr<RepTask>&)>& _callback,
            const uint64_t& _block_size,
            const uint64_t& _end_off):
            start_off(_start),
            cur_off(_start),
            path(_path),
            RepTask(_id,_vol_id,_j_counter,_is_opened,_status,_ts,
                _callback,_block_size,_end_off){
                init();
    }
    ~JournalTask(){
        if(is.is_open())
            is.close();
    }

    bool has_next_block() override;

    int get_next_block(int64_t& counter,char* data,uint64_t& off,
                        const size_t& len) override;

    int init() override;
};

// TODO:split to multiple journal files of SnapTask
class DiffSnapTask:public RepTask {
private:
    std::string pre_snap;
    std::string cur_snap;

    std::vector<DiffBlocks> diff_blocks;
    int vector_cursor; // cursor of DiffBlocks vector
    int array_cursor; // cursor of array in DiffBlock
    bool traverse_all; // traverse all diff blocks?
    uint64_t cur_off; // pair with j_counter, indicate whether there was space in journal
    uint64_t max_journal_size;
    char* buffer;
public:
    DiffSnapTask():RepTask(){}
    DiffSnapTask(const std::string& _pre,
            const std::string& _cur):
            RepTask(){}
    DiffSnapTask(const std::string& _pre,
            std::string& _cur,
            const uint64_t& _id,
            const std::string& _vol_id,
            const uint64_t& _j_counter,
            const bool& _is_opened,
            const TaskStatus& _status,
            const uint64_t _ts,
            std::function<void(std::shared_ptr<RepTask>&)>& _callback,
            const uint64_t& _block_size,
            const uint64_t& _end_off):
            pre_snap(_pre),
            cur_snap(_cur),
            RepTask(_id,_vol_id,_j_counter,_is_opened,_status,_ts,
                _callback,_block_size,_end_off){
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

    bool has_next_block() override;

    int get_next_block(int64_t& counter,
                        char* data,uint64_t& off,const size_t& len) override;

    int init() override;
};

class BaseSnapTask:public RepTask {
private:
    std::string base_snap;
    uint64_t vol_size;
public:
    BaseSnapTask():RepTask(){}
    BaseSnapTask(const std::string& _base,
                      const uint64_t& _vol_size):
                base_snap(_base),
                vol_size(_vol_size),
                RepTask(){}
    BaseSnapTask(const std::string& _base,
            const uint64_t& _id,
            const std::string& _vol_id,
            const uint64_t& _j_counter,
            const bool& _is_opened,
            const TaskStatus& _status,
            const uint64_t _ts,
            std::function<void(std::shared_ptr<RepTask>&)>& _callback,
            const uint64_t& _block_size,
            const uint64_t& _end_off):
            base_snap(_base),
            RepTask(_id,_vol_id,_j_counter,_is_opened,_status,_ts,
                _callback,_block_size,_end_off){
                init();
    }
    ~BaseSnapTask(){}

    std::string& get_base_snap();
    void set_base_snap(const std::string& _snap);

    uint64_t get_vol_size();
    void set_vol_size(const uint64_t& _size);

    bool has_next_block() override;

    int get_next_block(int64_t& counter,char* data,uint64_t& off,
                        const size_t& len) override;

    int init() override;
};
#endif