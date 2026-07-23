# Switchboard

Event-driven relay engine over [mamamia](https://github.com/yellowpages-ink/mamamia).
Relays GitHub repository activity into Discord, and serves Discord slash commands.

Internals are a **sense → decide → act** pipeline over two durable logs
(`obs`, `cmd`): **Sensor** turns the outside world into observations, **Decider**
turns observations into commands, **Actuator** executes commands against the
world and reports a result observation, **Tap** reads a log and records.

## Design
See [docs/superpowers/specs/2026-07-21-switchboard-design.md](docs/superpowers/specs/2026-07-21-switchboard-design.md).

## Scripts

| script | when |
|---|---|
| `./scripts/dev.sh [run\|test\|shell]` | local development |
| `./scripts/deploy.sh` | first deploy on a host (idempotent) |
| `./scripts/update.sh` | redeploy an existing host |
| `./scripts/vendor-mamamia.sh` | rebuild the pinned mamamia wheel into `vendor/` |

## Develop
```bash
./scripts/dev.sh test        # venv + deps if needed, then pytest
./scripts/dev.sh run         # app on 127.0.0.1:8199, data in .devdata/
```
It creates the venv, installs mamamia editable from `../mamamia` (falling back to
the vendored wheel), and always overrides `SB_PORT`/`SB_DATA_DIR` so a dev run
can't collide with a deployment.

## Run (Docker, on a host)
1. Vendor the pinned mamamia wheel — it is not on any index and `vendor/` is
   gitignored, so no `git pull` ever refreshes it:
   ```bash
   ./scripts/vendor-mamamia.sh          # MAMAMIA_DIR=../mamamia by default
   ```
   No mamamia checkout on the host? Build it elsewhere and `scp` the wheel into
   `vendor/`.
2. `cp .env.example .env`, fill it in, `chmod 600 .env`.
   `GITHUB_WEBHOOK_SECRET` is required; the Discord block is optional — see below.
3. `./scripts/deploy.sh` — checks prerequisites, validates `.env`, builds,
   starts, and waits for `/health` before reporting success.
4. Point the GitHub webhook at your hostname's `/webhook/github`, content-type
   `application/json`, same secret, events `pull_request`,
   `pull_request_review`, `issues`, `check_run`.

Afterwards, `./scripts/update.sh` pulls, re-checks the wheel, rebuilds, restarts
and re-verifies health.

### Ingress

This stack does **not** ship a tunnel. It publishes the app on **loopback only**
(`127.0.0.1:8080` — never the LAN) and stops there; getting traffic to the host
is the host's job.

Point whatever you already run — cloudflared, nginx, caddy — at:

```
http://localhost:8080
```

GitHub's webhook then targets that public hostname's `/webhook/github`. Nothing in this
repo needs tunnel credentials.

## Discord (optional)
Leave `DISCORD_BOT_TOKEN` empty to run GitHub-only — no bot, no slash commands,
nothing relayed into Discord. When it is set, `DISCORD_APPLICATION_ID` is
required (the app fails fast at startup without it).

| var | effect |
|---|---|
| `DISCORD_BOT_TOKEN` | wires the gateway bot, `/ping` + `/echo`, and the reply actuator |
| `DISCORD_APPLICATION_ID` | required alongside the token; used for interaction followups |
| `DISCORD_GUILD_ID` | registers slash commands instantly for that guild; unset syncs globally (~1h) |
| `DISCORD_GITHUB_NOTIFY_CHANNEL_ID` | channel the `github-notify` decider posts GitHub events into; unset = no relay |

Invite the bot with the `bot` + `applications.commands` scopes. Channel posts
need **Send Messages** in the target channel; interaction replies do not.

## Inspect dead letters
```bash
python -m switchboard.cli dead-letters --db ./data/events.db
```
