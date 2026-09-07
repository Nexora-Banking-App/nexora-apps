import os
import uuid
import time
import jwt
import httpx
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

# --- Configuration ---
JWT_SECRET = os.getenv("JWT_SECRET", "nexora-super-secret-key-change-in-prod")
AUTH_URL = os.getenv("AUTH_SVC_URL", "http://auth-service:8000")
ACCOUNT_URL = os.getenv("ACCOUNT_SVC_URL", "http://account-service:8000")
TRANSACTION_URL = os.getenv("TRANSACTION_SVC_URL", "http://transaction-service:8000")
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", "nexora-internal-secret-key-123")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:8080,http://127.0.0.1:8080").split(",")

# --- Rate Limiter ---
class RateLimiter:
    def __init__(self) -> None:
        self.requests = defaultdict(list)

    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> bool:
        now = time.time()
        self.requests[key] = [t for t in self.requests[key] if t > now - window_seconds]
        if len(self.requests[key]) >= max_requests:
            return False
        self.requests[key].append(now)
        return True

rate_limiter = RateLimiter()

# --- Application Lifecycle ---
http_client: httpx.AsyncClient

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        timeout=10.0, 
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=200)
    )
    yield
    await http_client.aclose()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Middleware ---
@app.middleware("http")
async def gateway_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    client_ip = request.client.host if request.client else "unknown"

    is_internal_test = request.headers.get("X-Internal-Service-Key") == INTERNAL_SERVICE_SECRET
    path = request.url.path

    if not is_internal_test:
        if path == "/api/login" and request.method == "POST":
            if not rate_limiter.is_allowed(f"login:{client_ip}", max_requests=5, window_seconds=60):
                return JSONResponse(status_code=429, content={"detail": "Too many login attempts. Please try again in 60 seconds."})

        if path == "/api/signup" and request.method == "POST":
            if not rate_limiter.is_allowed(f"signup:{client_ip}", max_requests=3, window_seconds=3600):
                return JSONResponse(status_code=429, content={"detail": "Signup quota exceeded for this IP. Try again later."})

    if path.startswith("/api/account") or path.startswith("/api/transfer"):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(status_code=401, content={"detail": "Missing or invalid Authorization Bearer token"})
        
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            request.state.user_id = str(payload["user_id"])
            request.state.username = str(payload["username"])
        except jwt.ExpiredSignatureError:
            return JSONResponse(status_code=401, content={"detail": "Session expired. Please log in again."})
        except jwt.InvalidTokenError:
            return JSONResponse(status_code=401, content={"detail": "Invalid token signature."})

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    
    if "server" in response.headers:
        del response.headers["server"]

    return response

# --- Reverse Proxy Logic ---
async def proxy_request(target_url: str, request: Request) -> Response:
    headers = {
        "X-Correlation-ID": getattr(request.state, "correlation_id", str(uuid.uuid4())),
        "Content-Type": "application/json"
    }
    
    if hasattr(request.state, "user_id"):
        headers["X-User-Id"] = request.state.user_id
        headers["X-Username"] = request.state.username

    if "Idempotency-Key" in request.headers:
        headers["Idempotency-Key"] = request.headers["Idempotency-Key"]

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

# --- Health Probes for Kubernetes Lifecycle ---
@app.get("/health/liveness")
def liveness(): 
    return {"status": "alive"}

@app.get("/health/readiness")
def readiness(): 
    return {"status": "ready"}

# --- Routes ---
@app.post("/api/signup")
async def signup(request: Request): return await proxy_request(f"{AUTH_URL}/signup", request)

@app.post("/api/login")
async def login(request: Request): return await proxy_request(f"{AUTH_URL}/login", request)

@app.get("/api/account/me")
async def get_account(request: Request): return await proxy_request(f"{ACCOUNT_URL}/account/me", request)

@app.post("/api/transfer")
async def transfer(request: Request): return await proxy_request(f"{TRANSACTION_URL}/transfer", request)