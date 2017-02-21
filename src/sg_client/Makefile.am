noinst_PROGRAMS=sg_client
AM_CPPFLAGS=-I$(top_srcdir)/src -I$(top_srcdir)/src/include \
            -I$(top_srcdir)/src/rpc -std=c++11
ACLOCAL_AMFLAGS=-I m4
sg_client_SOURCES=$(top_srcdir)/src/common/crc32.c  \
                  $(top_srcdir)/src/common/xxhash.c \
                  $(top_srcdir)/src/common/utils.cc \
                  $(top_srcdir)/src/common/volume_attr.cc \
                  $(top_srcdir)/src/sg_server/ceph_s3_lease.cc \
                  $(top_srcdir)/src/sg_server/ceph_s3_api.cc   \
                  control/control_snapshot.cc  \
                  control/control_backup.cc    \
                  control/control_replicate.cc \
                  control/control_volume.cc \
                  connection.cc       \
                  io_service_pool.cc  \
                  seq_generator.cc    \
                  journal_entry.cc    \
                  journal_writer.cc   \
                  journal_replayer.cc \
                  journal_reader.cc   \ 
                  journal_preprocessor.cc    \
                  volume.cc           \
                  volume_manager.cc   \
                  server.cc           \
                  main.cc  
  
sg_client_LDADD=${top_srcdir}/src/rpc/librpc.la \
                ${top_srcdir}/src/log/liblog.la \
                $(top_srcdir)/src/sg_client/cache/libcache.la \
                $(top_srcdir)/src/snapshot/libsnapshot.la     \
                $(top_srcdir)/src/backup/libbackup.la

sg_client_LDFLAGS=-D_GNU_SOURCE
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK -DNDEBUG
LDFLAGS="-L$(top_srcdir)/lib"
LIBS=-lpthread -lprotobuf -lgrpc -lgrpc++ -ls3 \
     -lboost_system -lboost_log_setup -lboost_log \ 
     -lboost_date_time -lboost_thread -lboost_filesystem

SUBDIRS=../rpc      \
        ../snapshot \
        ../backup   \
        cache
