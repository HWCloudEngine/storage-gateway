noinst_PROGRAMS=sg_tool
AM_CPPFLAGS=-I$(top_srcdir)/src/sg_server -I$(top_srcdir)/src/rpc -std=c++11
sg_tool_LDFLAGS=-D_GNU_SOURCE
sg_tool_SOURCES=sg_tool.cc
sg_tool_LDADD=$(top_srcdir)/src/rpc/librpc.la \
			  $(top_srcdir)/src/log/liblog.la

DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG
LIBS=-lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++ \
	 -lboost_system -lboost_log_setup -lboost_log -lboost_unit_test_framework
SUBDIRS=../log
