#ifndef JOURNAL_REQUEST_HANDLER_HPP
#define JOURNAL_REQUEST_HANDLER_HPP

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include "message.hpp"
#include "journal_writer.hpp"

namespace Journal{

typedef boost::shared_ptr<JournalWriter> journal_writer;
class RequestHandler
	:private boost::noncopyable
{
public:
	explicit RequestHandler();
	void handle_request(const Request& req,Reply& rep);

private:
	journal_writer journal_writer_ptr;
};

}

#endif