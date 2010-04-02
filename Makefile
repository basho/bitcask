.PHONY: rel deps

all: deps compile

compile:
	./rebar compile eunit

deps:
	./rebar get-deps

clean:
	./rebar clean
