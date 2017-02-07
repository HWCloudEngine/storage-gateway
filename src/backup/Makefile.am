noinst_LTLIBRARIES=libbackup.la
libbackup_la_SOURCES=../common/block_store.cc  \
                     ../common/index_store.cc  \
                     ../common/utils.cc  \
					 backup_proxy.cc     \
					 backup_decorator.cc \
                     backup_mds.cc       \
                     backup_mgr.cc    

AM_CPPFLAGS= -I$(top_srcdir)/src/rpc -std=c++11 -g

libbackup_la_LIBADD=${top_srcdir}/src/rpc/librpc.la \
					${top_srcdir}/src/snapshot/libsnapshot.la

#set boost lib path
LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/usr/local/lib"
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lboost_system -lrt -lboost_filesystem -lpthread -lrados -lrocksdb -lz \
     -lsnappy -lbz2 -lgrpc -lgrpc++
SUBDIRS=../rpc \
		../snapshot
