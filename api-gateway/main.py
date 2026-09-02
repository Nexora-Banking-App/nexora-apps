from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from contextlib import asynccontextmanager
import httpx
import os
import uuid
import jwt
import time
from collections import defaultdict

JWT_SECRET = os.getenv("JWT_SECRET", "nexora-super-secret-key-change-in-prod")
AUTH_URL = os.getenv("AUTH_SVC_URL", "http://auth-service:8000")
ACCOUNT_URL = os.getenv("ACCOUNT_SVC_URL", "http://account-service:8000")
TRANSACTION_URL = os.getenv("TRANSACTION_SVC_URL", "http://transaction-service:8000")
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", "nexora-internal-secret-key-123")

# -----------------------------------------------------------------------------
# 1. PERIMETER SLIDING-WINDOW RATE LIMITER (In-Memory)
# -----------------------------------------------------------------------------
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)

    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> bool:
        now = time.time()
        # Evict timestamps older than the sliding window
        self.requests[key] = [t for t in self.requests[key] if t > now - window_seconds]
        if len(self.requests[key]) >= max_requests:
            return False
        self.requests[key].append(now)
        return True

rate_limiter = RateLimiter()

# -----------------------------------------------------------------------------
# 2. LIFESPAN CONTEXT HANDLER (Graceful Connection Draining)
# -----------------------------------------------------------------------------
http_client: httpx.AsyncClient = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        timeout=10.0, 
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=200)
    )
    yield
    # Triggered on SIGTERM: Drain active HTTP sockets before termination
    await http_client.aclose()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

# -----------------------------------------------------------------------------
# 3. CORS SPECIFICATION (Explicit Origins with Credentials Support)
# -----------------------------------------------------------------------------
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS", 
    "http://localhost:8080,http://127.0.0.1:8080,http://localhost,http://127.0.0.1"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# 4. EDGE GATEWAY MIDDLEWARE (CORS Bypass, Tracing, Rate-Limits, Auth)
# -----------------------------------------------------------------------------
@app.middleware("http")
async def gateway_middleware(request: Request, call_next):
    # CRITICAL: Always bypass authentication for browser CORS preflight OPTIONS
    if request.method == "OPTIONS":
        return await call_next(request)

    # 1. Distributed Tracing: Assign or propagate Correlation ID
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    client_ip = request.client.host if request.client else "unknown"

    # 2. Check if caller is an authorized internal service or automated test runner
    is_internal_test = request.headers.get("X-Internal-Service-Key") == INTERNAL_SERVICE_SECRET

    # 3. Rate Limiting (Bypassed for authorized CI/CD test runners)
    if not is_internal_test:
        path = request.url.path
        if path == "/api/login" and request.method == "POST":
            if not rate_limiter.is_allowed(f"login:{client_ip}", max_requests=5, window_seconds=60):
                return Response(
                    content='{"detail":"Too many login attempts. Please try again in 60 seconds."}', 
                    status_code=429, 
                    media_type="application/json"
                )

        if path == "/api/signup" and request.method == "POST":
            if not rate_limiter.is_allowed(f"signup:{client_ip}", max_requests=3, window_seconds=3600):
                return Response(
                    content='{"detail":"Signup quota exceeded for this IP. Try again later."}', 
                    status_code=429, 
                    media_type="application/json"
                )

    # 4. Edge JWT Authentication & Claim Extraction on Protected Routes
    path = request.url.path
    if path.startswith("/api/account") or path.startswith("/api/transfer"):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return Response(
                content='{"detail":"Missing or invalid Authorization Bearer token"}', 
                status_code=401, 
                media_type="application/json"
            )
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            # Strip untrusted headers and store verified identity in request state
            request.state.user_id = str(payload["user_id"])
            request.state.username = str(payload["username"])
        except jwt.ExpiredSignatureError:
            return Response(
                content='{"detail":"Session expired. Please log in again."}', 
                status_code=401, 
                media_type="application/json"
            )
        except jwt.InvalidTokenError:
            return Response(
                content='{"detail":"Invalid token signature."}', 
                status_code=401, 
                media_type="application/json"
            )

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response

# -----------------------------------------------------------------------------
# 5. REVERSE PROXY DISPATCHER
# -----------------------------------------------------------------------------
async def proxy_request(target_url: str, request: Request):
    headers = {
        "X-Correlation-ID": getattr(request.state, "correlation_id", str(uuid.uuid4())),
        "Content-Type": "application/json"
    }
    # Propagate trusted identity claims to internal microservices
    if hasattr(request.state, "user_id"):
        headers["X-User-Id"] = request.state.user_id
        headers["X-Username"] = request.state.username

    # Propagate client idempotency keys
    if "Idempotency-Key" in request.headers:
        headers["Idempotency-Key"] = request.headers["Idempotency-Key"]

    # Propagate internal service authorization if present
    if "X-Internal-Service-Key" in request.headers:
        headers["X-Internal-Service-Key"] = request.headers["X-Internal-Service-Key"]

    body = await request.body()
    try:
        res = await http_client.request(
            method=request.method,
            url=target_url,
            headers=headers,
            content=body,
            params=request.query_params
        )
        return Response(content=res.content, status_code=res.status_code, media_type="application/json")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Upstream gateway timeout")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream service unavailable: {str(e)}")

# -----------------------------------------------------------------------------
# 6. ROUTE DEFINITIONS
# -----------------------------------------------------------------------------
@app.post("/api/signup")
async def signup(request: Request):
    return await proxy_request(f"{AUTH_URL}/signup", request)

@app.post("/api/login")
async def login(request: Request):
    return await proxy_request(f"{AUTH_URL}/login", request)

@app.get("/api/account/me")
async def get_account(request: Request):
    return await proxy_request(f"{ACCOUNT_URL}/account/me", request)

@app.post("/api/transfer")
async def transfer(request: Request):
    return await proxy_request(f"{TRANSACTION_URL}/transfer", request)