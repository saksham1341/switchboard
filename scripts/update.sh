#!/usr/bin/env bash
# Redeploy Switchboard on a host: fetch, verify the vendored wheel, rebuild,
# restart, and confirm it actually came up.
#
#   ./scripts/update.sh              # update to origin/main and restart
#   BRANCH=some-branch ./scripts/update.sh
#   ./scripts/update.sh --no-pull    # rebuild/restart current checkout only
#
# Safe to re-run. Refuses to clobber uncommitted work. Exits non-zero if the
# service does not answer /health, so it fails loudly instead of leaving a dead
# container behind a green-looking command.
set -euo pipefail

cd "$(dirname "$0")/.."
BRANCH="${BRANCH:-main}"
PULL=1
[ "${1:-}" = "--no-pull" ] && PULL=0

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

[ -f docker-compose.yml ] || die "run from the switchboard checkout (no docker-compose.yml here)"
[ -f .env ] || die ".env missing — cp .env.example .env, fill it, chmod 600 .env"

# --- 1. update source ----------------------------------------------------
if [ "$PULL" = 1 ]; then
  say "updating to origin/$BRANCH"
  if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    die "working tree has uncommitted changes to tracked files; commit/stash or use --no-pull"
  fi
  # --depth 1 keeps this working on the shallow clones deploy hosts usually have.
  # Reset to FETCH_HEAD, not origin/$BRANCH: a --single-branch clone only tracks
  # the cloned branch, so fetching any other one populates FETCH_HEAD and no
  # remote-tracking ref at all.
  git fetch --depth 1 --quiet origin "$BRANCH"
  git reset --hard --quiet FETCH_HEAD
fi
echo "at $(git rev-parse --short HEAD) — $(git log -1 --pretty=%s)"

# --- 2. vendored mamamia wheel ------------------------------------------
# The Dockerfile installs one pinned wheel and vendor/ is gitignored, so a plain
# git update never refreshes it. Catch the mismatch here rather than 40 lines
# into a failed docker build.
WANT="$(grep -oE 'mamamia-[0-9]+\.[0-9]+\.[0-9]+-py3-none-any\.whl' Dockerfile | head -1)"
[ -n "$WANT" ] || die "could not read the pinned mamamia wheel name from Dockerfile"

if [ ! -f "vendor/$WANT" ]; then
  say "vendored wheel missing or stale (need $WANT)"
  if [ -d "${MAMAMIA_DIR:-../mamamia}" ]; then
    ./scripts/vendor-mamamia.sh
  else
    echo "have: $(ls vendor/*.whl 2>/dev/null || echo 'nothing')"
    die "no mamamia checkout to build from (set MAMAMIA_DIR).
       Build it elsewhere and copy it over:
         (cd /path/to/mamamia && python -m build --wheel)
         scp /path/to/mamamia/dist/$WANT <host>:$(pwd)/vendor/"
  fi
fi
echo "wheel ok: vendor/$WANT"

# --- 3. rebuild + restart ------------------------------------------------
say "rebuilding and restarting"
docker compose up -d --build

# --- 4. verify -----------------------------------------------------------
PORT="$(grep -E '^SB_HOST_PORT=' .env | cut -d= -f2)"; PORT="${PORT:-8080}"
say "waiting for health on 127.0.0.1:$PORT"
for i in $(seq 1 30); do
  if curl -fsS -m 3 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    echo "healthy after ${i}s"
    HEALTHY=1; break
  fi
  sleep 1
done

if [ "${HEALTHY:-0}" != 1 ]; then
  echo "--- last 30 log lines ---" >&2
  docker compose logs --tail=30 switchboard >&2 || true
  die "service did not become healthy in 30s"
fi

docker compose ps --format '{{.Name}}  {{.State}}  {{.Status}}' 2>/dev/null || docker compose ps

# --- 5. surface dead letters (silent failures otherwise) -----------------
DEAD="$(docker compose exec -T switchboard python -m switchboard.cli dead-letters --db /data/events.db 2>/dev/null | wc -l || echo 0)"
if [ "$DEAD" -gt 0 ]; then
  printf '\n\033[33mnote: %s dead-lettered deliver%s — inspect with:\033[0m\n' "$DEAD" "$([ "$DEAD" = 1 ] && echo y || echo ies)"
  echo "  docker compose exec switchboard python -m switchboard.cli dead-letters --db /data/events.db"
fi

say "done"
