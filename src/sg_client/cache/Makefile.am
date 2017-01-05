noinst_LTLIBRARIES=libcache.la
libcache_la_SOURCES=bcache.cc        \
					cache_proxy.cc   \
					cache_recover.cc \
					common.cc        \
				    jcache.cc
AM_CPPFLAGS=-std=c++11 -g
#set boost lib path
LDFLAGS="-L/usr/lib/x86_64-linux-gnu/"
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lrt -lboost_filesystem -lpthread
