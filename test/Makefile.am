bin_PROGRAMS=debug_client journal_meta_utils
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

journal_meta_utils_SOURCES=journal_meta_utils.cc \
                    ../src/common/config_option.cc \
                    ../src/common/ceph_s3_api.cc \
                    ../src/common/index_store.cc \
                    ../src/common/db_meta_api.cc
journal_meta_utils_LDADD=$(LDADD) -ls3 -lrados \
						-lrocksdb -lz -lsnappy -lbz2  -lzstd