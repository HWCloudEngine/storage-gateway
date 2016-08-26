noinst_PROGRAMS=recover
recover_SOURCES=main.cpp cache_recover.cpp 
recover_CPPFLAGS=-std=c++11
recover_LDFLAGS=-D_GNU_SOURCE
#INCLUDES=-l../
#noinst_HEADERS
#recover_LDADD=$(top_srcdir)/src/rpc/librpc.la
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lboost_system  -lboost_thread
