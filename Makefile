NAME=teleport
VERSION=0.0.1-alpha.6
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

## release: build all binaries and release to GitHub
release: deb rpm build xbinary binary
	@echo Releasing $(NAME) $(VERSION)
	@hub release create v$(VERSION) \
		-a pkg/teleport_$(VERSION)_amd64.deb \
		-a pkg/teleport_$(subst -,_,$(VERSION))_x86_64.rpm \
		-a pkg/$(NAME)_$(VERSION).macos.tbz \
		-a pkg/$(NAME)_$(VERSION).linux-x86_64.tar.gz \
		-o

## drelease: build a docker image and release to DockerHub
drelease: dbuild
	@docker tag $(NAME):v$(VERSION) teleportdata/$(NAME):v$(VERSION)
	@docker tag $(NAME):v$(VERSION) teleportdata/$(NAME):latest
	@docker push teleportdata/$(NAME):v$(VERSION)
	@docker push teleportdata/$(NAME):latest

## build: build the binary
build: clean prepare
	@mkdir -p pkg/$(VERSION)/
	@go build $(LDFLAGS) -o pkg/$(VERSION)/$(NAME)

## build: build the binary for linux/amd64
xbuild: clean prepare
	@mkdir -p pkg/$(VERSION).amd64
	@GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o pkg/$(VERSION).amd64/$(NAME)
	@# brew install upx
	@upx -qq ./pkg/$(VERSION).amd64/$(NAME)

## dbuild: build the docker image
dbuild:
	@docker build -t $(NAME):v$(VERSION) .

## binary: package the binary
binary: build
	@mkdir -p pkg/$(NAME)_$(VERSION).macos
	@cp pkg/$(VERSION)/$(NAME) pkg/$(NAME)_$(VERSION).macos/
	@cp README.md pkg/$(NAME)_$(VERSION).macos/
	@pushd pkg && tar cvfj $(NAME)_$(VERSION).macos.tbz $(NAME)_$(VERSION).macos && popd

## xbinary: package a linux binary
xbinary: xbuild
	@mkdir -p pkg/$(NAME)_$(VERSION).linux-x86_64
	@cp ./pkg/$(VERSION).amd64/$(NAME) pkg/$(NAME)_$(VERSION).linux-x86_64/
	@cp README.md pkg/$(NAME)_$(VERSION).linux-x86_64/
	@pushd pkg && tar zcvf $(NAME)_$(VERSION).linux-x86_64.tar.gz $(NAME)_$(VERSION).linux-x86_64 && popd

## deb: build a deb package for debian/ubuntu
deb: xbuild
	@docker run --rm -v $(shell pwd):/data skandyla/fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p /data/pkg/teleport_VERSION_ARCH.deb \
		/data/pkg/$(VERSION).amd64/$(NAME)=/usr/bin/teleport

## rpm: build an rpm package for redhat/centos
rpm: xbuild
	@docker run --rm -v $(shell pwd):/data skandyla/fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p /data/pkg/teleport_VERSION_ARCH.rpm \
		/data/pkg/$(VERSION).amd64/$(NAME)=/usr/bin/teleport

## stats: print code statistics
stats:
	@echo "LOC:       \c"
	@find . -name "*.go" ! -name "*_test.go" ! -name "bindata.go" | xargs cat | wc -l
	@echo "Test LOC: \c" 
	@find . -name "*_test.go" | xargs cat | wc -l

udocker:
	@echo "Run setup commands in container:"
	@echo "  apt-get update && apt-get install -y build-essential curl git"
	@echo "  dpkg -i /data/pkg/teleport_$(VERSION)_amd64.deb"
	@docker run -it --rm -v $(shell pwd):/data -v $(shell pwd)/test/testpad:/pad -e PADPATH=/pad ubuntu:focal

deb_reload:
	apt-get purge -y $(NAME)
	dpkg -i teleport_$(VERSION)_amd64.deb

cdocker:
	@echo "Run setup commands in container:"
	@echo "  yum groupinstall \"Development Tools\" -y"
	@echo "  yum install -y /data/pkg/teleport_$(subst -,_,$(VERSION))_x86_64.rpm"
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
