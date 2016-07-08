#include "connection_manager.hpp"
#include <boost/bind.hpp>
#include <algorithm>
#include "connection.hpp"

namespace Journal{

void ConnectionManager::start(connection_ptr c)
{
	connections.insert(c);
	c->start();
}

void ConnectionManager::stop(connection_ptr c)
{
	connections.erase(c);
	c->stop();
}

void ConnectionManager::stop_all()
{
	/*
	for(connection_ptr c:connections)
	{
		c->stop();
	}*/

	std::for_each(connections.begin(),connections.end(),
	              boost::bind(&Connection::stop,_1));
	connections.clear();
}
}