# Switchboard

Event-driven relay engine over [mamamia](https://github.com/yellowpages-ink/mamamia).
v1 relays GitHub repository activity into a structured log (Discord egress is v1.1).

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
1. Build the mamamia wheel and vendor it:
   ```bash
   (cd ../mamamia && python -m build)
   mkdir -p vendor && cp ../mamamia/dist/mamamia-0.2.0-py3-none-any.whl vendor/
   ```
2. `cp .env.example .env` and fill in `GITHUB_WEBHOOK_SECRET` and `TUNNEL_TOKEN`
   (`chmod 600 .env`).
3. `docker compose up -d --build`
4. Point the GitHub webhook at the tunnel hostname's `/webhook`, content-type
   `application/json`, with the same secret.

The `switchboard` app port (8080) is internal-only and is not published to the
host. In the Cloudflare Zero Trust dashboard, configure the tunnel's public
hostname to route to the origin `http://switchboard:8080` (the compose service
name/port); GitHub's webhook then points at that public hostname's `/webhook`.

## Inspect dead letters
```bash
python -m switchboard.cli dead-letters --db ./data/events.db
```
