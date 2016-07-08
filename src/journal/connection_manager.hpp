#ifndef JOURNAL_CONNECTION_MANAGER_HPP
#define JOURNAL_CONNECTION_MANAGER_HPP

#include <set>
#include <boost/noncopyable.hpp>
#include "connection.hpp"

namespace Journal{

class ConnectionManager
	:private boost::noncopyable
{
	public:
		void start(connection_ptr con);
		void stop(connection_ptr con);
		void stop_all();
	private:
		std::set<connection_ptr> connections;
};
}

#endif	