noinst_PROGRAMS=sg_server
AM_CPPFLAGS=-I$(top_srcdir)/src -I$(top_srcdir)/src/include \
            -I$(top_srcdir)/src/rpc -std=c++11
ACLOCAL_AMFLAGS=-I m4
sg_server_SOURCES=dr_server.cc \
                  consumer_service.cc \
                  writer_service.cc \
                  ceph_s3_meta.cc \
                  ceph_s3_api.cc \
                  gc_task.cc \
                  ceph_s3_lease.cc \
                  volume_inner_control.cc \
                  replicate/rep_scheduler.cc \
                  replicate/rep_receiver.cc \
                  replicate/rep_transmitter.cc \
                  replicate/rep_inner_ctrl.cc \
                  replicate/rep_volume.cc \
                  replicate/replicator_context.cc \
                  replicate/markers_maintainer.cc \
                  replicate/rep_task_generator.cc \
                  ../common/utils.cc       \
                  ../common/block_store.cc \
                  ../common/index_store.cc \
                  ../common/volume_attr.cc \
                  backup/backup_util.cc \
                  backup/backup_ctx.cc  \
                  backup/backup_task.cc \
                  backup/backup_mds.cc  \
                  backup/backup_mgr.cc  \
                  snapshot/snapshot_util.cc \
                  snapshot/snapshot_mds.cc \
                  snapshot/snapshot_mgr.cc 

sg_server_LDADD=${top_srcdir}/src/rpc/librpc.la \
                 ${top_srcdir}/src/log/liblog.la 

sg_server_LDFLAGS=-D_GNU_SOURCE -L$(top_srcdir)/lib -L/usr/local/lib 
DEFS+=-D_GNU_SOURCE -DBOOST_ALL_DYN_LINK
LIBS=-lpthread -ldl \
     -lboost_system -lboost_log_setup -lboost_log -lboost_date_time -lboost_thread \
     -lprotobuf -ldl -lgrpc -lgrpc++ \
     -ls3 -lrados \
     -lrocksdb -lz -lsnappy -lbz2
SUBDIRS=../rpc 
