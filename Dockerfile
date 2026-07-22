FROM python:3.12-slim

WORKDIR /app

# mamamia (pinned v0.2.0) as a vendored wheel — see README.
COPY vendor/ /app/vendor/
COPY pyproject.toml /app/
COPY switchboard/ /app/switchboard/

RUN pip install --no-cache-dir /app/vendor/mamamia-0.2.0-py3-none-any.whl \
    && pip install --no-cache-dir .

ENV SB_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8080

CMD ["python", "-m", "switchboard.app"]
