#!/usr/bin/env bash
# Build the pinned mamamia wheel from the sibling checkout and vendor it.
#
# mamamia is not published to an index, so the Docker image installs it from
# vendor/. That directory is gitignored — run this before `docker compose build`
# (and again after bumping mamamia's version).
set -euo pipefail

MAMAMIA_DIR="${MAMAMIA_DIR:-../mamamia}"
VERSION="${MAMAMIA_VERSION:-0.2.0}"
WHEEL="mamamia-${VERSION}-py3-none-any.whl"

if [ ! -d "$MAMAMIA_DIR" ]; then
  echo "error: mamamia checkout not found at '$MAMAMIA_DIR'" >&2
  echo "       set MAMAMIA_DIR=/path/to/mamamia" >&2
  exit 1
fi

echo "building wheel from $MAMAMIA_DIR ..."
( cd "$MAMAMIA_DIR" && python -m build --wheel )

if [ ! -f "$MAMAMIA_DIR/dist/$WHEEL" ]; then
  echo "error: expected $MAMAMIA_DIR/dist/$WHEEL after build" >&2
  echo "       built wheels:" >&2
  ls -1 "$MAMAMIA_DIR/dist/"*.whl >&2 2>/dev/null || true
  echo "       set MAMAMIA_VERSION to match, or bump the pin in the Dockerfile" >&2
  exit 1
fi

mkdir -p vendor
cp "$MAMAMIA_DIR/dist/$WHEEL" vendor/
echo "vendored vendor/$WHEEL"
