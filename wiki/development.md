If you have a Go development environment set up, it will be quick and easy to get started.

# Linux

1. Clone this repo locally: `git clone https://github.com/teleport-data/teleport`
2. Install Go 1.14 or greater.
3. Run `make prepare` to download Go dependencies
4. Run `make test` to execute the test suite to verify compilation.
5. Run `make build` to compile a binary.
6. Run `make rpm` or `make deb` to build RPM/DEB files.

# OSX

1. Clone this repo locally: `git clone https://github.com/teleport-data/teleport`
2. Install Go `brew install go`
3. Run `make prepare` to download other Go dependencies
4. Run `make test` to execute the test suite to verify compilation.
5. Run `make build` to compile a binary.
