bin_PROGRAMS=sg_client
AM_CXXFLAGS=-std=c++11
AM_CPPFLAGS=-I$(top_srcdir)/src -I$(top_srcdir)/src/include \
            -I$(top_srcdir)/src/rpc
ACLOCAL_AMFLAGS=-I m4
AM_LDFLAGS = -rdynamic
sg_client_SOURCES=../common/crc32.c  \
                  ../common/xxhash.c \
                  ../common/utils.cc \
                  ../common/config_option.cc \
                  ../common/atomic_ptr.cc    \
                  ../common/env_posix.cc     \
                  ../common/timer.cc         \
                  ../common/block_store.cc   \
                  ../common/lease_client.cc \
                  ../common/journal_entry.cc \
                  ../rpc/clients/rpc_client.cc \
                  ../common/volume_attr.cc   \
                  control/control_snapshot.cc  \
                  control/control_backup.cc    \
                  control/control_replicate.cc \
                  control/control_volume.cc  \
                  control/control_agent.cc   \
                  control/control_iscsi.cc   \
                  snapshot/snapshot_proxy.cc  \
                  snapshot/snapshot_rpccli.cc \
                  backup/backup_rpccli.cc \
                  backup/backup_decorator.cc \
                  backup/backup_proxy.cc \
                  cache/bcache.cc        \
                  cache/cache_proxy.cc   \
                  cache/cache_recover.cc \
                  cache/common.cc        \
                  cache/jcache.cc        \
                  replicate_proxy.cc  \
                  perf_counter.cc     \
                  seq_generator.cc    \
                  server_socket.cc    \
                  client_socket.cc    \
                  journal_writer.cc   \
                  journal_replayer.cc \
                  journal_reader.cc   \
                  journal_preprocessor.cc \
                  journal_marker.cc \
                  volume.cc         \
                  volume_manager.cc \
                  main.cc
  
sg_client_LDADD=${top_srcdir}/src/rpc/librpc.la \
                ${top_srcdir}/src/log/liblog.la 

sg_client_LDFLAGS=-D_GNU_SOURCE  -L/usr/local/lib
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG
LIBS=-lpthread -lrt \
     -lboost_system -lboost_log_setup -lboost_log \
     -lboost_date_time -lboost_thread -lboost_filesystem \
     -lprotobuf -lgrpc -lgrpc++ \
     -ls3 -lrados

SUBDIRS=../rpc
