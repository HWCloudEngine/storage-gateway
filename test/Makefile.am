bin_PROGRAMS=debug_client libs3_utils
AM_CXXFLAGS=-std=c++11
AM_CPPFLAGS=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG \
            -I$(top_srcdir)/src -I$(top_srcdir)/src/rpc

LDADD=$(top_srcdir)/src/rpc/librpc.la \
      $(top_srcdir)/src/log/liblog.la \
      -lpthread -lprotobuf -lpthread -ldl -lgrpc -lgrpc++ \
      -lboost_system -lboost_log_setup -lboost_log

debug_client_SOURCES=debug_client.cc \
                     ../src/common/config_option.cc \
                     ../src/common/utils.cc

libs3_utils_SOURCES=libs3_utils.cc \
                    ../src/common/config_option.cc \
                    ../src/common/ceph_s3_api.cc
libs3_utils_LDADD=$(LDADD) -ls3
