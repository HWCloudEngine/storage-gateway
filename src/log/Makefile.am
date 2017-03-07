noinst_LTLIBRARIES=liblog.la
liblog_la_SOURCES=log.cc
AM_CPPFLAGS=-std=c++11
#set boost lib path
AM_LDFLAGS = -rdynamic
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lboost_log_setup -lboost_log -lboost_date_time -lboost_thread -lboost_filesystem -lpthread
