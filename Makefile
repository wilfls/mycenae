#
# Mycenae project
#
SHELL := /bin/bash
COVER := /tmp/mycenae.cover

#
# Targets
#
default: build
build install:
	@echo " === Running: $@ === "
	@godep go "$@" -a -v -o mycenae
	@echo
test $(COVER): $(wildcard *.go)
	@godep go test -v -cover -coverprofile="$(COVER)"
cover: $(COVER) $(wildcard *.go)
	@godep go tool cover -html="$(COVER)"

run: build
	@"./mycenae" -config=config.toml

clean:
	@godep go clean

rpm-build:
	@echo " === Generating RPM === "
	@rpmbuild -bb ../rpm/mycenae.spec
	@echo " === RPM Generated === "
