/*
 * journal_replayer.hpp
 *
 *  Created on: 2016��7��14��
 *      Author: smile-luobin
 */

#ifndef JOURNAL_JOURNAL_REPLAYER_HPP_
#define JOURNAL_JOURNAL_REPLAYER_HPP_

#include <string>

namespace Journal
{

class JournalReplayer
{
	//todo
public:
	void replay(const std::string& vol_id);
};

}

#endif /* JOURNAL_JOURNAL_REPLAYER_HPP_ */
