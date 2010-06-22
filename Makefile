.PHONY: rel deps

BITCASK_TAG		= $(shell hg identify -t)

all: deps compile

compile:
	./rebar compile eunit

deps:
	./rebar get-deps

clean:
	./rebar clean

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary

distdir:
	$(if $(findstring tip,$(BITCASK_TAG)),$(error "You can't generate a release tarball from tip"))
	mkdir distdir
	hg clone -u $(BITCASK_TAG) . distdir/bitcask-clone
	cd distdir/bitcask-clone; \
	hg archive ../$(BITCASK_TAG)

dist $(BITCASK_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(BITCASK_TAG).tar.gz $(BITCASK_TAG)

distclean:
	rm -rf $(BITCASK_TAG).tar.gz distdir

