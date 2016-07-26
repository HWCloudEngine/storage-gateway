/*
 * replay_handler.cpp
 *
 *  Created on: 2016Äê7ÔÂ26ÈÕ
 *      Author: smile-luobin
 */

#include <mutex>
#include <thread>
#include "replay_handler.hpp"

namespace Journal{

std::mutex vol_list_mutex;

ReplayHandler::ReplayHandler(int interval_time):
		interval_time(interval_time){

}

void ReplayHandler::handle_replay(){
	// TODO: use multi_thread
	while(true){
		vol_list_mutex.lock();
		std::list<std::string> vol_list(this->vol_list);
		vol_list_mutex.unlock();
		for(auto vol_id: vol_list){
			this->journal_replayer.replay(vol_id);
		}
		std::this_thread::sleep_for(std::chrono::seconds(this->interval_time));
	}
}

void ReplayHandler::start_replay(){
	std::thread replay_thread(&ReplayHandler::handle_replay, *this);
	replay_thread.detach();
}

void ReplayHandler::add_vol(const std::string& vol_id){
	ReplayHandler& replay_handler = *this;
	std::thread add_thread([&vol_id, &replay_handler](){
		vol_list_mutex.lock();
		replay_handler.vol_list.push_back(vol_id);
		vol_list_mutex.unlock();
	});
	add_thread.join();
}

void ReplayHandler::remove_vol(const std::string& vol_id){
	ReplayHandler& replay_handler = *this;
	std::thread remove_thread([&vol_id, &replay_handler](){
		vol_list_mutex.lock();
		replay_handler.vol_list.remove(vol_id);
		vol_list_mutex.unlock();
	});
	remove_thread.join();
}

}
