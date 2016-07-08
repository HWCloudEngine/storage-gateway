#include <iostream>
#include <string>
#include "server.hpp"

int main(int argc, char* argv[])
{
	if (argc != 3)
	{
		std::cerr << "Usage:journal_server <address> <port>";
		return 1;
	}
	try
	{
		Journal::Server server(argv[1], argv[2]);
		server.run();
	}
	catch (std::exception& e)
	{
		std::cerr <<"exception:" << e.what() << "\n";
	}

	return 0;
}
