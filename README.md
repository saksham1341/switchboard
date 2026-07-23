# Switchboard

Event-driven relay engine over [mamamia](https://github.com/yellowpages-ink/mamamia).
Relays GitHub repository activity into Discord, and serves Discord slash commands.

Internals are a **sense → decide → act** pipeline over two durable logs
(`obs`, `cmd`): **Sensor** turns the outside world into observations, **Decider**
turns observations into commands, **Actuator** executes commands against the
world and reports a result observation, **Tap** reads a log and records.

## Design
See [docs/superpowers/specs/2026-07-21-switchboard-design.md](docs/superpowers/specs/2026-07-21-switchboard-design.md).

## Develop
```bash
python3.12 -m venv venv && . venv/bin/activate
pip install -e ../mamamia          # co-developed sibling checkout
pip install -e ".[dev]"
python -m pytest -q
```

## Run (Docker, on the Pi)
1. Vendor the pinned mamamia wheel (it is not on any index, and `vendor/` is
   gitignored — re-run after bumping mamamia):
   ```bash
   ./scripts/vendor-mamamia.sh          # MAMAMIA_DIR=../mamamia by default
   ```
2. `cp .env.example .env` and fill it in (`chmod 600 .env`).
   `GITHUB_WEBHOOK_SECRET` is required; the Discord block is optional — see below.
3. `docker compose up -d --build`
4. Point the GitHub webhook at the tunnel hostname's `/webhook`, content-type
   `application/json`, with the same secret.

### Ingress

This stack does **not** ship a tunnel. It publishes the app on **loopback only**
(`127.0.0.1:8080` — never the LAN) and stops there; getting traffic to the host
is the host's job.

Point whatever you already run — cloudflared, nginx, caddy — at:

```
http://localhost:8080
```

GitHub's webhook then targets that public hostname's `/webhook`. Nothing in this
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
