PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
HASH := $(shell git rev-parse HEAD)
TAG ?= "sdfs/sdfs-proxy:$(VERSION)"

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

getdeps:
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.27.0)

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps fmt lint

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on gofmt -d cmd/
	@GO111MODULE=on gofmt -d pkg/

lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=5m --config ./.golangci.yml



# Builds sdfs-proxy locally.
build:
	@mkdir -p $(PWD)/build
	@echo "Building sdfs-proxy binary to '$(PWD)/build/sdfs-proxy'"
	@go build -ldflags="-X 'main.Version=$(BRANCH)' -X 'main.BuildDate=$$(date -Iseconds)'" -o ./build/sdfs-proxy cmd/server/* 

# Builds sdfs-proxy and installs it to $GOPATH/bin.
install: build
	@echo "Installing sdfs-proxy binary to '$(GOPATH)/bin/sdfs-proxy'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/build/sdfs-proxy $(GOPATH)/bin/sdfs-proxy
	@echo "Installation successful. To learn more, try \"sdfs-proxy\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf sdfs-proxy
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*
