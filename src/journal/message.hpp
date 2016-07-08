#ifndef JOURNAL_MESSAGE_HPP
#define JOURNAL_MESSAGE_HPP

namespace Journal{

struct Request
{
	int version;
	//todo
};

struct Reply
{
	enum status_type
	{
		OK = 200,
		ERROR = 201
	}status;
	//todo
};

}
#endif
