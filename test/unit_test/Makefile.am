check_PROGRAMS = sg_server_ut sg_client_ut
TESTS = $(check_PROGRAMS)

AM_LDFLAGS = -L/usr/local/lib

LDADD = \
    -lgtest_main \
    -lgtest \
    -lgmock \
    -ldl \
    -lboost_system -lboost_log_setup -lboost_log -lboost_date_time -lboost_thread \
    -lprotobuf -lgrpc -lgrpc++ \
    $(top_srcdir)/src/rpc/librpc.la \
    ${top_srcdir}/src/log/liblog.la

AM_CPPFLAGS = -std=c++11 \
    -D_GNU_SOURCE \
    -DBOOST_ALL_DYN_LINK\
    -I$(top_srcdir)/src \
    -I$(top_srcdir)/src/include \
    -I$(top_srcdir)/src/rpc

sg_server_ut_SOURCES = \
    sg_server/consumer_service_test.cc \
    ../../src/sg_server/consumer_service.cc \
    sg_server/writer_service_test.cc

sg_client_ut_SOURCES = \
    sg_client/journal_writer_test.cc
        
