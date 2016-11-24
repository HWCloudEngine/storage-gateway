noinst_LTLIBRARIES=libsnapshot.la
libsnapshot_la_SOURCES= block_store.cpp     \
						index_store.cpp     \
						snapshot_proxy.cpp  \
						snapshot_mgr.cpp    \
						snapshot_mds.cpp 
AM_CPPFLAGS=-std=c++11 -g
#set boost lib path
LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/usr/local/lib"
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lrt -lboost_filesystem -lpthread -lrados -lrocksdb -lz \
	 -lsnappy
