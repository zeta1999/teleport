#!/bin/bash
#
# Teleport installer
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/teleport-data/teleport/master/scripts/install.sh | bash

# When releasing Teleport, the releaser should update this version number
# AFTER they upload new binaries.
VERSION="0.0.1-alpha.2"
BREW=$(command -v brew)

set -e

function copy_binary() {
  if [[ ":$PATH:" == *":$HOME/.local/bin:"* ]]; then
      mv teleport "$HOME/.local/bin/teleport"
  else
      echo "Installing Teleport to /usr/local/bin which is write protected"
      echo "If you'd prefer to install Teleport without sudo permissions, add \$HOME/.local/bin to your \$PATH and rerun the installer"
      sudo mv teleport /usr/local/bin/teleport
  fi
}

function install_teleport() {
  if [[ "$OSTYPE" == "linux-gnu" ]]; then
      set -x
      curl -fsSL https://github.com/hundredwatt/teleport/releases/download/v$VERSION/teleport_$VERSION.linux-x86_64.tar.gz | tar -xzv teleport_$VERSION.linux-x86_64/teleport
      mv teleport_$VERSION.linux-x86_64/teleport teleport
      rmdir teleport_$VERSION.linux-x86_64/
      copy_binary
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    #if [[ "$BREW" != "" ]]; then
    #    set -x
    #    brew tap teleport-data/tap
    #    brew install teleport-data/tap/teleport
    #else
        set -x
        curl -fsSL https://github.com/hundredwatt/teleport/releases/download/v$VERSION/teleport_$VERSION.macos.tbz | tar -xzv teleport_$VERSION.macos/teleport
        mv teleport_$VERSION.macos/teleport teleport
        rmdir teleport_$VERSION.macos/
        copy_binary
    #fi
  else
    set +x
    echo "The Teleport installer does not work for your platform: $OS"
    echo "Please file an issue at https://github.com/teleport-dev/teleport/issues/new"
    exit 1
  fi

  set +x
}

function version_check() {
  VERSION="$(teleport version 2>&1 || true)"
  TELEPORT_VERSION_PATTERN='^ Teleport [0-9]+\.[0-9]+\.[0-9]'
  if ! [[ $VERSION =~ $TELEPORT_VERSION_PATTERN ]]; then
    echo "Teleport installed!"
    echo
    echo "Note: it looks like it is not the first program named 'teleport' in your path. \`teleport version\` (running from $(command -v teleport)) did not return a teleport version string."
    echo "It output this instead:"
    echo
    echo "$VERSION"
    echo
    echo "Perhaps you have a different program named teleport in your \$PATH?"
    exit 1
  else
    echo "Teleport installed!"
  fi
}

# so that we can skip installation in CI and just test the version check
if [[ -z $NO_INSTALL ]]; then
  install_teleport
fi

version_check
