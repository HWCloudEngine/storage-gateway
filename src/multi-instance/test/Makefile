all:test.cc tooz_client.cc simple_callback.cc
	g++ -g test.cc tooz_client.cc simple_callback.cc -Wno-write-strings -I/usr/include/python2.7 -lpthread -lpython2.7 -L/usr/lib/python2.7 -std=c++11 -o multi_instance
clean:
	rm -rf *.o multi_instance *.pyc
