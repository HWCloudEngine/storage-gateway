noinst_PROGRAMS=sg_client
AM_CPPFLAGS=-I$(top_srcdir)/src -I$(top_srcdir)/src/include \
            -I$(top_srcdir)/src/rpc -std=c++11
ACLOCAL_AMFLAGS=-I m4
sg_client_SOURCES=../common/crc32.c  \
                  ../common/xxhash.c \
                  ../common/utils.cc \
                  ../common/config.cc \
                  ../common/volume_attr.cc   \
                  ../common/block_store.cc   \
                  ../common/ceph_s3_lease.cc \
                  ../common/ceph_s3_api.cc   \
                  ../common/journal_entry.cc    \
                  control/control_snapshot.cc  \
                  control/control_backup.cc    \
                  control/control_replicate.cc \
                  control/control_volume.cc  \
                  snapshot/snapshot_proxy.cc \
                  backup/backup_decorator.cc \
                  backup/backup_proxy.cc \
                  cache/bcache.cc        \
                  cache/cache_proxy.cc   \
                  cache/cache_recover.cc \
                  cache/common.cc        \
                  cache/jcache.cc        \
                  replicate_proxy.cc  \
                  connection.cc       \
                  io_service_pool.cc  \
                  seq_generator.cc    \
                  journal_writer.cc   \
                  journal_replayer.cc \
                  journal_reader.cc   \
                  journal_preprocessor.cc \
                  volume.cc               \
                  volume_manager.cc   \
                  server.cc           \
                  main.cc  
  
sg_client_LDADD=${top_srcdir}/src/rpc/librpc.la \
                ${top_srcdir}/src/log/liblog.la 

sg_client_LDFLAGS=-D_GNU_SOURCE -L$(top_srcdir)/lib -L/usr/local/lib
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG
LIBS=-lpthread \
     -lboost_system -lboost_log_setup -lboost_log \
     -lboost_date_time -lboost_thread -lboost_filesystem \
     -lprotobuf -lgrpc -lgrpc++ \
     -ls3 -lrados

SUBDIRS=../rpc
