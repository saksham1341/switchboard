#!/usr/bin/env bash
# Local development. Ensures the venv and deps exist, then runs the app or the
# tests. Never touches a deployed instance: it uses its own data dir and port.
#
#   ./scripts/dev.sh            # run the app on 127.0.0.1:8199, data in .devdata/
#   ./scripts/dev.sh test       # run the test suite (extra args passed to pytest)
#   ./scripts/dev.sh shell      # python REPL with the package importable
#
# Reads .env if present so real credentials work locally, but always overrides
# SB_PORT and SB_DATA_DIR so a dev run cannot collide with a deployment.
set -euo pipefail

cd "$(dirname "$0")/.."

VENV="venv"
PY="$VENV/bin/python"
DEV_PORT="${SB_DEV_PORT:-8199}"
DEV_DATA="${SB_DEV_DATA:-.devdata}"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

# --- venv + deps ---------------------------------------------------------
if [ ! -x "$PY" ]; then
  say "creating venv"
  PYBIN="$(command -v python3.12 || command -v python3 || true)"
  [ -n "$PYBIN" ] || die "no python3 found (need >= 3.12)"
  "$PYBIN" -m venv "$VENV"
fi

if ! "$PY" -c 'import switchboard, mamamia' >/dev/null 2>&1; then
  say "installing deps"
  "$PY" -m pip install --quiet --upgrade pip
  # mamamia is not on an index: prefer the co-developed sibling checkout so edits
  # there are live, else fall back to whatever wheel is vendored for Docker.
  if [ -d "${MAMAMIA_DIR:-../mamamia}" ]; then
    "$PY" -m pip install --quiet -e "${MAMAMIA_DIR:-../mamamia}"
    echo "mamamia: editable from ${MAMAMIA_DIR:-../mamamia}"
  elif ls vendor/mamamia-*.whl >/dev/null 2>&1; then
    "$PY" -m pip install --quiet vendor/mamamia-*.whl
    echo "mamamia: vendored wheel"
  else
    die "no mamamia source: clone it beside this repo, set MAMAMIA_DIR, or run ./scripts/vendor-mamamia.sh"
  fi
  "$PY" -m pip install --quiet -e ".[dev]"
fi

CMD="${1:-run}"; shift || true

case "$CMD" in
  test)
    exec "$PY" -m pytest -q "$@"
    ;;

  shell)
    exec "$PY"
    ;;

  run)
    # Real credentials if you have them, dev-safe defaults if you don't.
    if [ -f .env ]; then
      set -a; . ./.env; set +a
      echo "loaded .env"
    fi
    mkdir -p "$DEV_DATA"
    export SB_DATA_DIR="$DEV_DATA"
    export SB_PORT="$DEV_PORT"
    # The app hard-requires this; a dev run should not need a webhook secret.
    export GITHUB_WEBHOOK_SECRET="${GITHUB_WEBHOOK_SECRET:-dev-secret}"

    if [ -n "${DISCORD_BOT_TOKEN:-}" ]; then
      echo "discord: enabled (a second live bot session — stop other instances first)"
    else
      echo "discord: disabled (no DISCORD_BOT_TOKEN)"
    fi

    say "http://127.0.0.1:$SB_PORT  ·  data in $DEV_DATA  ·  ctrl-c to stop"
    exec "$PY" -m switchboard.app
    ;;

  *)
    die "unknown command '$CMD' (expected: run | test | shell)"
    ;;
esac
