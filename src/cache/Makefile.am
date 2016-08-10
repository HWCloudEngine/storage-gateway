noinst_PROGRAMS=cache
AM_CPPFLAGS=-std=c++11
cache_SOURCES=main.cpp proxy.cpp bcache.cpp jcache.cpp common.cpp
#cache_LDADD=$(top_srcdir)/src/rpc/librpc.la
cache_LDFLAGS=-D_GNU_SOURCE
DEFS+=-D_GNU_SOURCE
LIBS=-lpthread -lboost_system 
#SUBDIRS=../dr_server
