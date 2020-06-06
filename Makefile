VERSION := "alpha" # $(shell git describe --tags)
BUILD := $(shell git rev-parse --short HEAD)
PROJECTNAME := $(shell basename "$(PWD)")

# Use linker flags to provide version/build settings
LDFLAGS=-ldflags "-X=main.Version=$(VERSION) -X=main.Build=$(BUILD)"

## test: run all tests
test:
	go test

## build: build the binary
build:
	go build $(LDFLAGS) -o bin/$(PROJECTNAME)

## stats: print code statistics
stats:
	@echo "LOC:       \c"
	@ls -1 *.go | grep -v "_test.go" | xargs cat | wc -l
	@echo "Test LOC: \c" 
	@ls -1 *_test.go | xargs cat | wc -l

.PHONY: help test stats
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
