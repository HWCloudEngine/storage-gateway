/*
 * replay_handler.hpp
 *
 *  Created on: 2016Äê7ÔÂ26ÈÕ
 *      Author: smile-luobin
 */

#ifndef JOURNAL_REPLAY_HANDLER_HPP_
#define JOURNAL_REPLAY_HANDLER_HPP_

#include <list>
#include <string>
#include <boost/noncopyable.hpp>
#include "journal_replayer.hpp"

namespace Journal{

class ReplayHandler{
public:
	explicit ReplayHandler(int interval_time);
	void add_vol(const std::string& vol_id);
	void remove_vol(const std::string& vol_id);
	void handle_replay();
	void start_replay();
private:
	JournalReplayer journal_replayer;
	std::list<std::string> vol_list;
	int interval_time;
};

}


#endif /* JOURNAL_REPLAY_HANDLER_HPP_ */
