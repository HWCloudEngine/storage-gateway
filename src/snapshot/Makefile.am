noinst_LTLIBRARIES=libsnapshot.la
libsnapshot_la_SOURCES= block_store.cc     \
                        index_store.cc     \
                        snapshot_proxy.cc  \
						snapshot_util.cc   \
                        snapshot_mds.cc    \
						snapshot_vol.cc    \
                        snapshot_mgr.cc    
AM_CPPFLAGS= -I$(top_srcdir)/src/rpc -std=c++11 -g
#set boost lib path
LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/usr/local/lib"
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lrt -lboost_filesystem -lpthread -lrados -lrocksdb -lz \
     -lsnappy -lbz2
