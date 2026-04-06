FROM python:3.12-slim AS builder

WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir ".[notifications]"

FROM python:3.12-slim

RUN useradd -m -s /bin/bash magsync && \
    mkdir -p /config /data /magazines && \
    chown -R magsync:magsync /config /data /magazines

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/magsync /usr/local/bin/magsync

USER magsync
WORKDIR /home/magsync

ENV MAGSYNC_CONFIG_DIR=/config \
    MAGSYNC_DB_PATH=/data/index.db \
    MAGSYNC_OUTPUT_DIR=/magazines

VOLUME ["/config", "/data", "/magazines"]

HEALTHCHECK --interval=60s --timeout=5s --start-period=5m --retries=3 \
    CMD test $( find /tmp/magsync-healthy -mmin -720 2>/dev/null | wc -l ) -gt 0 || exit 1

CMD ["magsync", "daemon"]
