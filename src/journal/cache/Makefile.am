noinst_LTLIBRARIES=libcache.la
libcache_la_SOURCES=bcache.cpp cache_proxy.cpp cache_recover.cpp common.cpp \
				    jcache.cpp
AM_CPPFLAGS=-std=c++11
#set boost lib path
LDFLAGS="-L/usr/lib/x86_64-linux-gnu/"
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lrt -lboost_filesystem -lpthread
