FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir --no-compile --prefix=/install ".[notifications]"

FROM builder AS service-builder
RUN pip install --no-cache-dir --no-compile --prefix=/install ".[notifications,service]"

FROM python:3.12-slim AS runtime
RUN useradd -m -s /bin/bash magsync && \
    mkdir -p /config /data /magazines /exports && \
    chown -R magsync:magsync /config /data /magazines /exports
COPY --from=builder /install /usr/local
USER magsync
WORKDIR /home/magsync
ENV MAGSYNC_CONFIG_DIR=/config \
    MAGSYNC_DB_PATH=/data/index.db \
    MAGSYNC_OUTPUT_DIR=/magazines \
    MAGSYNC_EXPORT_DIR=/exports \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
VOLUME ["/config", "/data", "/magazines", "/exports"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-m", "magsync.companion.healthcheck"]

FROM runtime AS service
COPY --from=service-builder /install /usr/local
CMD ["magsync", "serve", "--host", "0.0.0.0"]

# Keep the ordinary/default build and deployment daemon-only.
FROM runtime AS daemon
CMD ["magsync", "daemon"]
