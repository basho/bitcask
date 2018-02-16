REPO		?= bitcask
BITCASK_TAG	 = $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
REBAR := $(shell which rebar)
ifeq ($(REBAR),)
REBAR = ./rebar3
endif

PULSE_TESTS = bitcask_pulse

.PHONY: rel deps package pkgclean

all: deps compile

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

BITCASK_IO_MODE=erlang

test: deps compile eunit_nif

eunit_nif:
	BITCASK_IO_MODE="nif" $(REBAR) skip_deps=true as test do eunit

NOW	= $(shell date +%s)
COUNTER = $(PWD)/$(NOW).current_counterexample.eqc
EQCINFO = $(PWD)/$(NOW).eqc-info

pulse:
	@rm -rf $(BASE_DIR)/.eunit
	BITCASK_PULSE=1 $(REBAR) clean compile
	env BITCASK_PULSE=1 $(REBAR) -D PULSE eunit skip_deps=true suites=$(PULSE_TESTS) ; \
	if [ $$? -ne 0 ]; then \
		echo PULSE test FAILED; \
		cp ./.eunit/current_counterexample.eqc $(COUNTER); \
		cp ./.eunit/.eqc-info $(EQCINFO); \
		echo See files $(COUNTER) and $(EQCINFO); \
		exit 1; \
	else \
		exit 0; \
	fi

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary
archivegit = git archive --format=tar --prefix=$(1)/ HEAD | (cd $(2) && tar xf -)
archivehg = hg archive $(2)/$(1)
archive = if [ -d ".git" ]; then \
		$(call archivegit,$(1),$(2)); \
	    else \
		$(call archivehg,$(1),$(2)); \
	    fi

buildtar = mkdir distdir && \
		 git clone . distdir/$(REPO)-clone && \
		 cd distdir/$(REPO)-clone && \
		 git checkout $(BITCASK_TAG) && \
		 $(call archive,$(BITCASK_TAG),..) && \
		 mkdir ../$(BITCASK_TAG)/deps && \
		 make deps; \
		 for dep in deps/*; do cd $${dep} && $(call archive,$${dep},../../../$(BITCASK_TAG)); cd ../..; done

distdir:
	$(if $(BITCASK_TAG), $(call buildtar), $(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))

dist $(BITCASK_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(BITCASK_TAG).tar.gz $(BITCASK_TAG)

ballclean:
	rm -rf $(BITCASK_TAG).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

export BITCASK_TAG REPO

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools \
				crypto inets xmerl webtool snmp public_key mnesia eunit \
				syntax_tools compiler

include tools.mk
