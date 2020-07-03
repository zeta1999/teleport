NAME=teleport
VERSION=0.0.1-alpha.1
BUILD=$(shell git rev-parse --short HEAD)

# Use linker flags to provide version/build settings
LDFLAGS=-ldflags "-X=main.Version=$(VERSION) -X=main.Build=$(BUILD)"

## clean: clean build directory with `go clean`
clean:
	@go clean

## prepare: get go dependencies
prepare:
	@go get -t

## test: run all tests
test:
	@go test

## private_release: release to S3 (until the GitHub repo is made public)
private_release: deb rpm xbinary binary
	@echo Releasing $(NAME) $(VERSION)
	@aws s3 cp --acl public-read teleport_$(VERSION)_amd64.deb s3://teleport-releases/v$(VERSION)/
	@aws s3 cp --acl public-read teleport_$(subst -,_,$(VERSION))_x86_64.rpm s3://teleport-releases/v$(VERSION)/
	@aws s3 cp --acl public-read tmp/$(NAME)_$(VERSION).macos.tbz s3://teleport-releases/v$(VERSION)/
	@aws s3 cp --acl public-read tmp/$(NAME)_$(VERSION).linux-x86_64.tar.gz s3://teleport-releases/v$(VERSION)/
	@aws s3 cp --acl public-read scripts/install.sh s3://teleport-releases/v$(VERSION)/
	@aws s3 cp --acl public-read s3://teleport-releases/v$(VERSION)/install.sh s3://teleport-releases/latest/install.sh

## release: build all binaries and release to
release: deb rpm build xbinary binary
	@echo Releasing $(NAME) $(VERSION)
	@hub release create v$(VERSION) \
		-a teleport_$(VERSION)_amd64.deb \
		-a teleport_$(subst -,_,$(VERSION))_x86_64.rpm \
		-a tmp/$(NAME)_$(VERSION).macos.tbz \
		-a tmp/$(NAME)_$(VERSION).linux-x86_64.tar.gz \
		-o

## build: build the binary
build: clean prepare
	@go build $(LDFLAGS) -o $(NAME)

## build: build the binary for linux/amd64
xbuild: clean prepare
	@GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(NAME)
	@# brew install upx
	@upx -qq ./teleport

## dbuild: build the docker image
dbuild: xbuild
	@docker build -t teleport .

## binary: package the binary
binary: build
	@mkdir -p tmp/$(NAME)_$(VERSION).macos
	@cp teleport tmp/$(NAME)_$(VERSION).macos/
	@cp README.md tmp/$(NAME)_$(VERSION).macos/
	@pushd tmp && tar cvfj $(NAME)_$(VERSION).macos.tbz $(NAME)_$(VERSION).macos && popd

## xbinary: package a linux binary
xbinary: xbuild
	@mkdir -p tmp/$(NAME)_$(VERSION).linux-x86_64
	@cp teleport tmp/$(NAME)_$(VERSION).linux-x86_64/
	@cp README.md tmp/$(NAME)_$(VERSION).linux-x86_64/
	@pushd tmp && tar zcvf $(NAME)_$(VERSION).linux-x86_64.tar.gz $(NAME)_$(VERSION).linux-x86_64 && popd

## deb: build a deb package for debian/ubuntu
deb: xbuild
	@docker run --rm -v $(shell pwd):/data skandyla/fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p /data/teleport_VERSION_ARCH.deb \
		/data/teleport=/usr/bin/teleport

## rpm: build an rpm package for redhat/centos
rpm: xbuild
	@docker run --rm -v $(shell pwd):/data skandyla/fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p /data/teleport_VERSION_ARCH.rpm \
		/data/teleport=/usr/bin/teleport

## stats: print code statistics
stats:
	@echo "LOC:       \c"
	@find . -name "*.go" ! -name "*_test.go" | xargs cat | wc -l
	@echo "Test LOC: \c" 
	@find . -name "*_test.go" | xargs cat | wc -l

udocker:
	@echo "Run setup commands in container:"
	@echo "  apt-get update"
	@echo "  apt-get install -y build-essential curl git"
	@echo "  dpkg -i teleport_$(VERSION)_amd64.deb"
	@docker run -it --rm -v $(shell pwd):/data -v $(shell pwd)/test/testpad:/pad -e PADPATH=/pad ubuntu:focal

deb_reload:
	apt-get purge -y $(NAME)
	dpkg -i teleport_$(VERSION)_amd64.deb

cdocker:
	@echo "Run setup commands in container:"
	@echo "  yum groupinstall \"Development Tools\" -y"
	@echo "  yum install -y teleport_$(subst -,_,$(VERSION))_x86_64.rpm"
	@docker run -it --rm -v $(shell pwd):/data -v $(shell pwd)/test/testpad:/pad -e PADPATH=/pad centos:centos8

rpm_reload:
	rpm -e $(NAME)
	yum install -y teleport_$(subst -,_,$(VERSION))_x86_64.rpm


.PHONY: help test stats
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(NAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
