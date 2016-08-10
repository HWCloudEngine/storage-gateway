noinst_PROGRAMS=cache
AM_CPPFLAGS=-std=c++11
cache_SOURCES=main.cpp cache_proxy.cpp bcache.cpp jcache.cpp common.cpp \
			  ../seq_generator.cpp
#cache_LDADD=$(top_srcdir)/src/rpc/librpc.la
cache_LDFLAGS=-D_GNU_SOURCE
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lboost_system  -lboost_thread
#SUBDIRS=../dr_server
