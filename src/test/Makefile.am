noinst_PROGRAMS=debug_client
AM_CPPFLAGS=-I$(top_srcdir)/src/sg_server -I$(top_srcdir)/src/rpc -std=c++11
debug_client_LDFLAGS=-D_GNU_SOURCE
debug_client_SOURCES=debug_client.cc
debug_client_LDADD=$(top_srcdir)/src/rpc/librpc.la \
                   $(top_srcdir)/src/log/liblog.la

DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++ \
	 -lboost_system -lboost_log_setup -lboost_log -lboost_unit_test_framework
SUBDIRS=../log
