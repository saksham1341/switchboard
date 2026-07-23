#!/usr/bin/env bash
# First-time deploy on a host. Idempotent — safe to re-run on a live host.
#
#   ./scripts/deploy.sh
#
# Checks prerequisites, scaffolds .env, then hands the build/restart/verify to
# scripts/update.sh so there is one implementation of that.
set -euo pipefail

cd "$(dirname "$0")/.."

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

# --- prerequisites -------------------------------------------------------
say "checking prerequisites"

command -v docker >/dev/null 2>&1 || die "docker not installed. Install it:
       curl -fsSL https://get.docker.com | sudo sh"

docker compose version >/dev/null 2>&1 || die "docker compose plugin missing (need Compose v2)"

if ! docker info >/dev/null 2>&1; then
  die "cannot talk to the docker daemon.
       Start it:      sudo systemctl enable --now docker
       Run unprivileged: sudo usermod -aG docker $USER   (then log out and back in)"
fi
echo "docker: $(docker --version | cut -d, -f1)"
echo "compose: $(docker compose version --short 2>/dev/null || echo present)"

# Survives reboot only if the daemon itself starts at boot.
if command -v systemctl >/dev/null 2>&1; then
  if [ "$(systemctl is-enabled docker 2>/dev/null || echo no)" != "enabled" ]; then
    printf '\033[33mwarning: docker is not enabled at boot — the stack will NOT come back after a reboot.\n         fix: sudo systemctl enable docker\033[0m\n'
  else
    echo "docker enabled at boot: yes"
  fi
fi

# --- configuration -------------------------------------------------------
say "checking configuration"

if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
  die ".env created from .env.example — fill it in, then re-run.
       Required: GITHUB_WEBHOOK_SECRET   (openssl rand -hex 32)
       Optional: the DISCORD_* block (leave DISCORD_BOT_TOKEN empty for GitHub-only)"
fi

if grep -q 'replace-me' .env; then
  die ".env still contains 'replace-me' placeholders — fill them in first"
fi

if ! grep -qE '^GITHUB_WEBHOOK_SECRET=.+' .env; then
  die "GITHUB_WEBHOOK_SECRET is empty in .env (openssl rand -hex 32)"
fi

# Discord is optional, but half-configured Discord fails fast at startup — say
# so here rather than in a crash loop.
if grep -qE '^DISCORD_BOT_TOKEN=.+' .env && ! grep -qE '^DISCORD_APPLICATION_ID=.+' .env; then
  die "DISCORD_BOT_TOKEN is set but DISCORD_APPLICATION_ID is empty — the app requires both"
fi
if grep -qE '^DISCORD_BOT_TOKEN=.+' .env; then
  echo "discord: enabled"
  grep -qE '^DISCORD_GITHUB_NOTIFY_CHANNEL_ID=.+' .env \
    || echo "  note: no notify channel set — slash commands only, GitHub events not relayed"
else
  echo "discord: disabled (GitHub-only)"
fi

chmod 600 .env
mkdir -p data

# --- build, start, verify ------------------------------------------------
./scripts/update.sh --no-pull

# --- what to do next -----------------------------------------------------
PORT="$(grep -E '^SB_HOST_PORT=' .env | cut -d= -f2)"; PORT="${PORT:-8080}"
say "deployed"
cat <<EOF
The app listens on 127.0.0.1:$PORT (loopback only — not the LAN).

Remaining wiring, both outside this repo:
  1. Point your ingress (cloudflared, nginx, caddy) at  http://localhost:$PORT
  2. Point the GitHub webhook at  https://<your-hostname>/webhook
     content-type application/json, secret = GITHUB_WEBHOOK_SECRET from .env,
     events: pull_request, pull_request_review, issues, check_run

Operate:
  logs          docker compose logs -f switchboard
  update        ./scripts/update.sh
  dead letters  docker compose exec switchboard python -m switchboard.cli dead-letters --db /data/events.db
EOF
