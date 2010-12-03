# requires GNU make
PKG		 = BASHO$(APP)
# possible ARCH values are i386, sparc, all
ARCH		 = $(shell uname -p)
SOLARIS_VER	?= $(shell echo "$(KERNEL)" | sed -e 's/^5\.//')
PKGFILE		 = $(PKG)-$(REVISION)-$(RELEASE)-$(DISTRO)$(SOLARIS_VER)-$(ARCH).pkg
BITCASK_LIB	?= lib/$(APP)-$(REVISION)

build: buildrel pkginfo prototype
	cp $(PKGERDIR)/copyright $(PKGERDIR)/depend .
	mkdir -p pkgbuild packages
	pkgmk -o -d pkgbuild -a $(ARCH)
	touch packages/$(PKGFILE)
	pkgtrans -s pkgbuild packages/$(PKGFILE) $(PKG) 
	rm -r pkgbuild/$(PKG)
	gzip packages/$(PKGFILE)
	@echo
	@echo Wrote: $(CURDIR)/packages/$(PKGFILE).gz
	@echo

# Build the release we need to package
buildrel:
	@# Ye Olde Bourne Shell on Solaris means we have to do it old school
	PATH=/opt/erlang/R13B04-$(DISTRO)$(SOLARIS_VER):$${PATH}; \
	export PATH; \
	echo "Using `which erl` to build"; \
	$(MAKE) -C $(BITCASK_PATH)

pkginfo:
	sed -e 's/@@VERSION@@/$(REVISION)-$(RELEASE)/g' \
		-e 's/@@PKG@@/$(PKG)/g' \
		-e 's/@@PKGNAME@@/$(APP)/g' \
		< $(PKGERDIR)/pkginfo.tmpl > pkginfo

# NOTE!  The instances of riak below shouldn't change
prototype:
	echo "i pkginfo" > prototype
	echo "i copyright" >> prototype
	echo "i depend" >> prototype
	echo '' >> prototype
	echo "d none lib/$(APP)-$(REVISION) 0755 riak riak" >> prototype
	pkgproto $(BITCASK_PATH)/ebin=$(BITCASK_LIB)/ebin >> prototype
	pkgproto $(BITCASK_PATH)/priv=$(BITCASK_LIB)/priv >> prototype
	sed -i -e 's/basho other/riak riak/' \
	       -e 's/buildbot other/riak riak/' prototype

$(PKGERDIR)/pkgclean:
	rm -rf copyright depend pkgbuild pkginfo prototype

