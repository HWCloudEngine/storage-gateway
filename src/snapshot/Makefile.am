noinst_LTLIBRARIES=libsnapshot.la
libsnapshot_la_SOURCES=../common/block_store.cc     \
                       ../common/index_store.cc     \
                       ../common/utils.cc \
                       snapshot_proxy.cc  \
                       snapshot_util.cc   \
                       snapshot_mds.cc    \
                       snapshot_mgr.cc    

AM_CPPFLAGS= -I$(top_srcdir)/src/rpc -std=c++11 -g

libsnapshot_la_LIBADD=${top_srcdir}/src/rpc/librpc.la 

#set boost lib path
LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/usr/local/lib"
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lrt -lboost_filesystem -lpthread -lrados -lrocksdb -lz \
     -lsnappy -lbz2 -lgrpc -lgrpc++
SUBDIRS=../rpc
