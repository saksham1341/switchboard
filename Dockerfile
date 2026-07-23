FROM python:3.12-slim

WORKDIR /app

# mamamia (pinned v0.2.0) as a vendored wheel — see README. Staged OUTSIDE the
# build dir: setuptools flat-layout auto-discovery scans top-level dirs of the
# project root, and a `vendor/` sitting next to `switchboard/` makes it abort
# with "Multiple top-level packages discovered in a flat-layout".
COPY vendor/ /tmp/vendor/
COPY pyproject.toml /app/
COPY switchboard/ /app/switchboard/

RUN pip install --no-cache-dir /tmp/vendor/mamamia-0.2.0-py3-none-any.whl \
    && pip install --no-cache-dir . \
    && rm -rf /tmp/vendor

ENV SB_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8080

CMD ["python", "-m", "switchboard.app"]
