"""Shared observability helpers: JSON logs, correlation IDs, request logging.

The SAME file is copied into every Python service folder (each service is its
own Docker build context, so they cannot import from a shared folder).
Edit api-gateway/nexora_common.py, then run scripts/sync-common.sh.
CI fails if the copies differ.

Only the standard library is required. OpenTelemetry is optional: when it is
installed and active, the current trace_id is added to every log line.
"""
import contextvars
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

try:  # optional: present in the images, absent in plain unit-test environments
    from opentelemetry import trace as _otel_trace
except Exception:  # pragma: no cover - depends on the environment
    _otel_trace = None

CORRELATION_HEADER = "X-Correlation-ID"

# Set once per request by the middleware below. Because they are contextvars,
# the values follow the request into `asyncio.to_thread()` workers and into
# FastAPI's thread pool, so every log line of a request carries them.
correlation_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "correlation_id", default=None
)
user_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "user_id", default=None
)

# Extra fields a log call may attach with `extra={...}`.
_EXTRA_FIELDS = ("method", "path", "status", "duration_ms", "user_id")

# Requests that are never logged (probe and scrape noise).
_QUIET_PATH_PREFIXES = ("/health", "/metrics")


def current_correlation_id() -> Optional[str]:
    return correlation_id_var.get()


def outbound_headers() -> dict:
    """Headers to add to calls to other services so the correlation ID travels."""
    cid = correlation_id_var.get()
    return {CORRELATION_HEADER: cid} if cid else {}


def _current_trace_id() -> Optional[str]:
    if _otel_trace is None:
        return None
    try:
        ctx = _otel_trace.get_current_span().get_span_context()
        if ctx and ctx.is_valid:
            return format(ctx.trace_id, "032x")
    except Exception:
        pass
    return None


class JsonFormatter(logging.Formatter):
    """One JSON object per line.

    Fields: timestamp, level, service, correlation_id, user_id (if known),
    method, path, status, duration_ms (access lines), message, trace_id (if tracing).
    Secrets are never added here: the formatter only emits the fields above,
    the log message itself and, for errors, the stack trace.
    """

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self.service,
            "correlation_id": correlation_id_var.get(),
            "message": record.getMessage(),
            "logger": record.name,
        }
        user_id = getattr(record, "user_id", None) or user_id_var.get()
        if user_id is not None:
            entry["user_id"] = str(user_id)
        for field in _EXTRA_FIELDS:
            if field == "user_id":
                continue
            value = getattr(record, field, None)
            if value is not None:
                entry[field] = value
        trace_id = _current_trace_id()
        if trace_id:
            entry["trace_id"] = trace_id
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(service: str) -> logging.Logger:
    """Route every logger (ours, uvicorn's, libraries') through the JSON formatter."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    # uvicorn installs its own plain-text handlers; send its messages to ours.
    for name in ("uvicorn", "uvicorn.error"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers = []
        uv_logger.propagate = True
    # The middleware below writes the access line, so uvicorn's is switched off
    # (the Dockerfile also passes --no-access-log).
    access = logging.getLogger("uvicorn.access")
    access.handlers = []
    access.propagate = False

    # httpx logs every outbound request at INFO; the access line already covers it.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logging.getLogger(service)


def install_request_logging(app, logger: logging.Logger) -> None:
    """Add the correlation-ID + access-log middleware to a FastAPI app.

    Call it AFTER every other `add_middleware` / `@app.middleware` so it is the
    outermost layer and sees the final status code.
    """

    @app.middleware("http")
    async def _request_logging(request, call_next):
        correlation_id = request.headers.get(CORRELATION_HEADER) or str(uuid.uuid4())
        cid_token = correlation_id_var.set(correlation_id)
        uid_token = user_id_var.set(request.headers.get("X-User-Id"))
        request.state.correlation_id = correlation_id

        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers[CORRELATION_HEADER] = correlation_id
            return response
        finally:
            path = request.url.path  # path only: never the query string or headers
            if not path.startswith(_QUIET_PATH_PREFIXES):
                logger.info(
                    "request",
                    extra={
                        "method": request.method,
                        "path": path,
                        "status": status_code,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                        # api-gateway learns the user from the JWT and stores it here
                        "user_id": getattr(request.state, "user_id", None) or user_id_var.get(),
                    },
                )
            correlation_id_var.reset(cid_token)
            user_id_var.reset(uid_token)


def warn_if_default_secret(logger: logging.Logger, name: str, value: str, development_default: str) -> None:
    """Log a warning when a secret still has its built-in development value.

    The defaults exist so `docker compose up` works with no setup. In Kubernetes
    the real values come from AWS Secrets Manager; this line in the logs means
    that wiring is broken and the service is running with a publicly known key.
    The secret itself is never logged.
    """
    if value == development_default:
        logger.warning("%s is using the built-in development default. Never run like this outside local development.", name)


def prepare_multiprocess_metrics() -> Optional[str]:
    """Opt-in Prometheus multiprocess mode (needed for `uvicorn --workers N`).

    Off unless PROMETHEUS_MULTIPROC_DIR is set. When it is set,
    prometheus_client stores samples in files in that directory and
    prometheus-fastapi-instrumentator's /metrics endpoint aggregates all worker
    processes through MultiProcessCollector. This function only makes sure the
    directory exists; it must run before prometheus_client is imported.
    The directory must be empty when the container starts (use an emptyDir).
    """
    directory = os.getenv("PROMETHEUS_MULTIPROC_DIR")
    if directory:
        os.makedirs(directory, exist_ok=True)
    return directory or None
