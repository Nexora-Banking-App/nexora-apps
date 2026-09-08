# STAGE 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
# CRITICAL FIX: Install requirements, then FORCE upgrade h11 to patch the CVE
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --upgrade h11==0.16.0

# STAGE 2: Runner
FROM python:3.11-slim

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY main.py .

RUN addgroup --system appgroup && adduser --system --group appuser
USER appuser

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]