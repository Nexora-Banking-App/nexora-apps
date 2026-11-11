# STAGE 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# STAGE 2: Runner
FROM python:3.11-slim

WORKDIR /app

# DEVSECOPS FIX: Pull the latest Debian security patches to fix perl-base CVEs!
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# main.py plus the shared helpers (nexora_common.py, rules.py, ...)
COPY *.py ./

# Unbuffered JSON logs; no .pyc writes (the root filesystem is read-only in Kubernetes).
# Tracing is off unless the deployment sets OTEL_SDK_DISABLED=false.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OTEL_SDK_DISABLED=true

RUN addgroup --system appgroup && adduser --system --group appuser
USER appuser

EXPOSE 8000
# opentelemetry-instrument adds tracing for FastAPI, httpx and PyMySQL (no-op while disabled).
# --no-access-log: the app writes its own JSON access line per request.
# Worker count: set WEB_CONCURRENCY (uvicorn reads it); see README "Multiple workers".
CMD ["opentelemetry-instrument", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]