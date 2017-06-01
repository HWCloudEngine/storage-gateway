# Run sharness tests
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#
# NOTE: run with TEST_VERBOSE=1 for verbose sharness tests.
# define SG_TESTS filter tests
ifdef SG_TESTS
    T = $(sort $(wildcard $(SG_TESTS)))
else
    T = $(sort $(wildcard t[0-9][0-9][0-9][0-9]-*.sh))
endif
# define SKIP_TESTS, to skip some tests
ifdef SKIP_TESTS
    SKIP_T = $(sort $(wildcard $(SKIP_TESTS)))
    SG_T = $(sort $(filter-out $(SKIP_T), $(T)))
else
    SG_T = $(T)
endif
LIBDIR = lib
SHARNESSDIR = sharness
AGGREGATE = $(LIBDIR)/$(SHARNESSDIR)/aggregate-results.sh


# Add below the binaries that this project generates or needs.
# For example:
# BINS = bin/ipfs
BINS =

all: aggregate

clean: clean-test-results
	@echo "*** $@ ***"
	# Clean binaries below.
	# For example:
	# -rm -rf bin/ipfs

clean-test-results:
	@echo "*** $@ ***"
	-rm -rf test-results trash*.sh

$(SG_T): clean-test-results deps
	@echo "*** $@ ***"
	./$@

aggregate: clean-test-results $(SG_T)
	@echo "*** $@ ***"
	ls test-results/t*-*.sh.*.counts | $(AGGREGATE)

# Add below some needed dependencies.
# For example:
# deps: sharness $(BINS) curl
deps: sharness $(BINS)

sharness:
	@echo "*** checking $@ ***"
	lib/install-sharness.sh

# Add below other targets like:
# - the targets needed to build binaries,
# - targets using special compile flags,
# - targets to check or install dependencies.
#
# For example:
#
# GOFLAGS =
#
# bin/%: FORCE
# 	cd .. && make GOFLAGS=$(GOFLAGS) $@
#
# race:
# 	make GOFLAGS=-race all
#
# curl:
#	@which curl >/dev/null || (echo "Please install curl!" && false)

.PHONY: all clean clean-test-results $(SG_T) aggregate deps sharness FORCE

