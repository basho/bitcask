.PHONY: compile rel cover test dialyzer eqc
REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

cover: test
	$(REBAR) cover

test: compile
	$(REBAR) eunit

dialyzer:
	$(REBAR) dialyzer

eqc:
	$(REBAR) eqc

xref:
	$(REBAR) xref

PULSE_TESTING_TIME ?= 30

pulse: compile
	mkdir -p .pulse
	cp eqc/pulse/Emakefile .pulse
	cp _build/default/lib/bitcask/ebin/bitcask.app .pulse
	(cd .pulse; \
		erl -make; \
		erl -noshell -s bitcask_pulse run_tests $(PULSE_TESTING_TIME))

check: test dialyzer xref
