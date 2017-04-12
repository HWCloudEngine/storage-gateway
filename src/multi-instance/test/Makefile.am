noinst_PROGRAMS=multi_instance
AM_CPPFLAGS=-I$(top_srcdir)/src/multi-instance -I$(top_srcdir)/src/log -I/usr/include/python2.7 -Wno-write-strings -std=c++11
multi_instance_LDFLAGS=-D_GNU_SOURCE -L$(top_srcdir)/lib -L/usr/local/lib -L/usr/lib/python2.7
multi_instance_SOURCES=multi_instance.cc \
                       ../tooz_client.cc \
                       simple_callback.cc
multi_instance_LDADD=$(top_srcdir)/src/log/liblog.la

DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG
LIBS=-lpthread -lpython2.7 -ldl -lboost_system -lboost_log_setup -lboost_log -lboost_unit_test_framework
