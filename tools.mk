REBAR ?= ./rebar

compile-no-deps:
	${REBAR} compile skip_deps=true

test: compile
	${REBAR} eunit skip_deps=true

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

PLT ?= $(HOME)/.combo_dialyzer_plt
LOCAL_PLT = .local_dialyzer_plt
DIALYZER_FLAGS ?= -Wunmatched_returns

${PLT}: compile
	@if [ -f $(PLT) ]; then \
		dialyzer --check_plt --plt $(PLT) --apps $(DIALYZER_APPS) && \
		dialyzer --add_to_plt --plt $(PLT) --output_plt $(PLT) --apps $(DIALYZER_APPS) ; test $$? -ne 1; \
	else \
		dialyzer --build_plt --output_plt $(PLT) --apps $(DIALYZER_APPS); test $$? -ne 1; \
	fi

${LOCAL_PLT}: compile
	@if [ -d deps ]; then \
		if [ -f $(LOCAL_PLT) ]; then \
			dialyzer --check_plt --plt $(LOCAL_PLT) deps/*/ebin  && \
			dialyzer --add_to_plt --plt $(LOCAL_PLT) --output_plt $(LOCAL_PLT) deps/*/ebin ; test $$? -ne 1; \
		else \
			dialyzer --build_plt --output_plt $(LOCAL_PLT) deps/*/ebin ; test $$? -ne 1; \
		fi \
	fi

dialyzer-run:
	@echo "==> $(shell basename $(shell pwd)) (dialyzer)"
# The bulk of the code below deals with the dialyzer.ignore-warnings file
# which contains strings to ignore if output by dialyzer.
# Typically the strings include line numbers. Using them exactly is hard
# to maintain as the code changes. This approach instead ignores the line
# numbers, but takes into account the number of times a string is listed
# for a given file (See the sort + uniq -c commands, which add the count
# as a prefix). So if one string is listed once, for example, and it
# appears twice in the warnings, the user is alerted. It is possible but
# unlikely that this approach could mask a warning if one ignored warning
# is removed and two warnings of the same kind appear in the file, for
# example. But it is a trade-off that seems worth it.
	@-if [ -f $(LOCAL_PLT) ]; then \
		PLTS="$(PLT) $(LOCAL_PLT)"; \
	else \
		PLTS=$(PLT); \
	fi; \
	if [ -f dialyzer.ignore-warnings ]; then \
		if [ $$(grep -cvE '[^[:space:]]' dialyzer.ignore-warnings) -ne 0 ]; then \
			echo "ERROR: dialyzer.ignore-warnings contains a blank/empty line, this will match all messages!"; \
			exit 1; \
		fi; \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin > dialyzer_warnings ; \
		sort dialyzer.ignore-warnings \
		| sed -E 's/^([^:]+:)[^:]+:/\1/' \
		| uniq -c \
		| sed -E '/.*\.erl: /!s/^[[:space:]]*[0-9]+[[:space:]]*//' \
		> dialyzer.ignore-warnings.tmp ; \
		egrep -v "^[[:space:]]*(done|Checking|Proceeding|Compiling)" dialyzer_warnings \
		| sort \
		| sed -E 's/^([^:]+:)[^:]+:/\1/' \
		| uniq -c \
		| sed -E '/.*\.erl: /!s/^[[:space:]]*[0-9]+[[:space:]]*//' \
		| grep -F -f dialyzer.ignore-warnings.tmp -v \
		| sed -E 's/^[[:space:]]*[0-9]+[[:space:]]*//' \
		> dialyzer_unhandled_warnings ; \
		rm dialyzer.ignore-warnings.tmp; \
		cat dialyzer_unhandled_warnings ; \
		[ $$(cat dialyzer_unhandled_warnings | wc -l) -eq 0 ] ; \
	else \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin; \
	fi

dialyzer-quick: compile-no-deps dialyzer-run

dialyzer: ${PLT} ${LOCAL_PLT} dialyzer-run

cleanplt:
	@echo
	@echo "Are you sure?  It takes several minutes to re-build."
	@echo Deleting $(PLT) and $(LOCAL_PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)
	rm $(LOCAL_PLT)

