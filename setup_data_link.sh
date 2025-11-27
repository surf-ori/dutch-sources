#!/usr/bin/env bash
# Sets up a symlink so writes to ./data go to the shared storage at
# ~/data/ori-storage/dutch-sources/data.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINK_PATH="$SCRIPT_DIR/data"
TARGET="${HOME}/data/ori-storage/dutch-sources/data"

if [ ! -d "$TARGET" ]; then
  echo "Target data directory not found: $TARGET" >&2
  exit 1
fi

if [ -L "$LINK_PATH" ]; then
  CURRENT_TARGET="$(readlink "$LINK_PATH")"
  if [ "$CURRENT_TARGET" = "$TARGET" ]; then
    echo "Symlink already points to target: $LINK_PATH -> $TARGET"
    exit 0
  else
    echo "Symlink exists but points to $CURRENT_TARGET; not touching" >&2
    exit 1
  fi
fi

if [ -e "$LINK_PATH" ]; then
  echo "Path exists and is not a symlink: $LINK_PATH. Remove or move it first." >&2
  exit 1
fi

ln -s "$TARGET" "$LINK_PATH"
echo "Created symlink: $LINK_PATH -> $TARGET"
