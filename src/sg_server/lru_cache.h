/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    lru_cache.h
* Author: 
* Date:         2017/01/25
* Version:      1.0
* Description:  cache the most recently inserted/updated key-values
* 
************************************************/
#ifndef LRU_CACHE_H_
#define LRU_CACHE_H_
#include <list>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <climits> // INT_MAX(32767)
#include <stdexcept> // range_error
#include <functional>

template <typename key_t,typename value_t>
bool default_get(const key_t& key,value_t& value){
    return false;
}

template <typename key_t,typename value_t>
class LruCache{
public:
    typedef typename std::pair<key_t,value_t> kv_pair_t;
    typedef typename std::list<kv_pair_t>::iterator list_it_t;
    // a function to get key-value if cache missed
    typedef bool (*pFn_get)(const key_t&,value_t&);

    LruCache(const int& size,pFn_get f):
        max_size_(size>0?size:INT_MAX),
        get_(f){}

    explicit LruCache(pFn_get f):
        LruCache(0,f){}

    LruCache(const int& size,std::function<bool(const key_t&,value_t&)> f):
        max_size_(size>0?size:INT_MAX),
        get_(f){}

    explicit LruCache(std::function<bool(const key_t&,value_t&)> f):
        LruCache(0,f){}

    explicit LruCache(const int& size):
        LruCache(size,default_get){}

    LruCache():LruCache(0,default_get){}

    const value_t& get(const key_t& key){
        std::unique_lock<std::mutex> lock(mtx_);
        auto it = map_.find(key);
        if(it == map_.end()){
            throw std::range_error("the key is not found in cache!");
        }
        else{
            list_.splice(list_.begin(),list_,it->second);
            return it->second->second;
        }
    }
    // first try to get from cache, if not hit in cache,
    // then try to get by user defined get function,
    // and update cache if got
    bool get(const key_t& key,value_t& value){
        std::unique_lock<std::mutex> lock(mtx_);
        auto it = map_.find(key);
        if(it == map_.end()){
            lock.unlock();
            if(get_(key,value)){
                put_if_not_exsit(key,value);
                return true;
            }
            else {
                return false;
            }
        }
        else{
            list_.splice(list_.begin(),list_,it->second);
            value = it->second->second;
            return true;
        }
    }

    void put(const key_t& key,const value_t& value){
        std::unique_lock<std::mutex> lock(mtx_);
        list_.push_front(kv_pair_t(key,value));
        auto it = map_.find(key);
        if(it != map_.end()){
            list_.erase(it->second);
            map_.erase(it);
        }
        // redirecting to new key-value iterator
        map_[key] = list_.begin();

        // if cache size exceed, drop the least access one
        if(map_.size() > max_size_){
            map_.erase(list_.crbegin()->first);
            list_.pop_back();
        }
    }

    void delete_key(const key_t& key){
        std::unique_lock<std::mutex> lock(mtx_);
        auto it = map_.find(key);
        if(it != map_.end()){
            list_.erase(it->second);
            map_.erase(it);
        }
    }

    void put_if_not_exsit(const key_t& key,const value_t& value){
        std::unique_lock<std::mutex> lock(mtx_);
        auto it = map_.find(key);
        if(it != map_.end()){
            return;
        }
        list_.push_front(kv_pair_t(key,value));
        map_[key] = list_.begin();

        // if cache size exceed, drop the least access one
        if(map_.size() > max_size_){
            map_.erase(list_.crbegin()->first);
            list_.pop_back();
        }
    }

    void clear(){
        std::unique_lock<std::mutex> lock(mtx_);
        list_.clear();
        map_.clear();
    }
private:
    std::mutex mtx_;
    int max_size_;
    std::list<kv_pair_t> list_;
    std::unordered_map<key_t,list_it_t> map_;
    std::function<bool(const key_t&,value_t&)> get_;
};
#endif
