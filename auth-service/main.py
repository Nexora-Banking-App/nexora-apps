from nexora_common import (
    configure_logging,
    install_request_logging,
    outbound_headers,
    prepare_multiprocess_metrics,
    warn_if_default_secret,
)

prepare_multiprocess_metrics()  # must run before prometheus_client is imported

import os
import asyncio
import random
import jwt
import httpx
import bcrypt
import pymysql
import datetime
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from dbutils.pooled_db import PooledDB

logger = configure_logging("auth-service")

# --- Configuration ---
_DEV_JWT_SECRET = "nexora-super-secret-key-change-in-prod"
_DEV_INTERNAL_SECRET = "nexora-internal-secret-key-123"
JWT_SECRET = os.getenv("JWT_SECRET", _DEV_JWT_SECRET)
JWT_ALGORITHM = "HS256"
TRANSACTION_SVC_URL = os.getenv("TRANSACTION_SVC_URL", "http://transaction-service:8000")
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", _DEV_INTERNAL_SECRET)
warn_if_default_secret(logger, "JWT_SECRET", JWT_SECRET, _DEV_JWT_SECRET)
warn_if_default_secret(logger, "INTERNAL_SERVICE_SECRET", INTERNAL_SERVICE_SECRET, _DEV_INTERNAL_SECRET)

# Welcome-grant retry: a background task re-sends grants that never settled
# (for example when transaction-service was unreachable during signup).
GRANT_RETRY_ENABLED = os.getenv("GRANT_RETRY_ENABLED", "true").lower() == "true"
GRANT_RETRY_INTERVAL_SECONDS = int(os.getenv("GRANT_RETRY_INTERVAL_SECONDS", "60"))
# Only users older than this are retried, so the retry never races a signup
# request that is still waiting for its own grant call.
GRANT_RETRY_MIN_AGE_SECONDS = int(os.getenv("GRANT_RETRY_MIN_AGE_SECONDS", "120"))
GRANT_RETRY_BATCH_SIZE = 50
TREASURY_USER_ID = 1

GRANT_RETRIES = Counter("welcome_grant_retries_total", "Welcome grants re-sent by the retry task", ["result"])

db_pool = PooledDB(
    creator=pymysql,
    maxconnections=10,
    mincached=0,
    maxcached=2,
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "root"),
    database=os.getenv("DB_NAME", "nexora_bank"),
    cursorclass=pymysql.cursors.DictCursor
)

http_client: httpx.AsyncClient


# --- Welcome grant ---
def grant_key(user_id: int) -> str:
    """Deterministic idempotency key: sending the same grant twice moves money once."""
    return f"grant-user-{user_id}"


async def request_welcome_grant(user_id: int) -> Optional[int]:
    """Asks transaction-service for the $1,000 grant.
    Returns the HTTP status code, or None if the service could not be reached."""
    headers = {"X-Internal-Service-Key": INTERNAL_SERVICE_SECRET}
    headers.update(outbound_headers())
    try:
        response = await http_client.post(
            f"{TRANSACTION_SVC_URL}/internal/system-transfer",
            headers=headers,
            json={
                "sender_id": TREASURY_USER_ID,
                "receiver_id": user_id,
                "amount": "1000.00",
                "idempotency_key": grant_key(user_id),
            },
        )
        return response.status_code
    except httpx.RequestError:
        return None


def _find_users_missing_grant() -> List[int]:
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT u.id
                FROM users u
                LEFT JOIN transactions t ON t.idempotency_key = CONCAT('grant-user-', u.id)
                WHERE t.id IS NULL
                  AND u.id <> %s
                  AND u.created_at < NOW() - INTERVAL %s SECOND
                ORDER BY u.id
                LIMIT %s
                """,
                (TREASURY_USER_ID, GRANT_RETRY_MIN_AGE_SECONDS, GRANT_RETRY_BATCH_SIZE),
            )
            rows = cursor.fetchall()
        conn.commit()
        return [row["id"] for row in rows]
    finally:
        conn.close()


async def _grant_retry_loop() -> None:
    """Every GRANT_RETRY_INTERVAL_SECONDS, re-send grants for users that have none.

    Safe on several replicas: every call uses the user's deterministic
    idempotency key, so transaction-service executes the grant at most once.
    A replica that loses the race just gets "already done" or "in flight" back.
    """
    logger.info("Welcome-grant retry task started (interval=%ss)", GRANT_RETRY_INTERVAL_SECONDS)
    while True:
        # Jitter keeps replicas from firing at the same instant.
        await asyncio.sleep(GRANT_RETRY_INTERVAL_SECONDS + random.uniform(0, 5))
        try:
            user_ids = await asyncio.to_thread(_find_users_missing_grant)
            for user_id in user_ids:
                code = await request_welcome_grant(user_id)
                if code == 200:
                    GRANT_RETRIES.labels(result="settled").inc()
                    logger.info("Welcome grant settled on retry for user_id=%s", user_id)
                elif code in (None, 409):
                    # Unreachable, or another replica is already on it: try again next round.
                    GRANT_RETRIES.labels(result="pending").inc()
                else:
                    GRANT_RETRIES.labels(result="failed").inc()
                    logger.warning("Welcome grant retry for user_id=%s returned HTTP %s", user_id, code)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Welcome-grant retry round failed; will try again")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    retry_task = asyncio.create_task(_grant_retry_loop()) if GRANT_RETRY_ENABLED else None
    yield
    if retry_task is not None:
        retry_task.cancel()
        try:
            await retry_task
        except asyncio.CancelledError:
            pass
    await http_client.aclose()
    db_pool.close()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)
install_request_logging(app, logger)

# --- Models ---
# Limits match the column sizes in db-init (username 50, email 100, phone 20),
# so oversized input is rejected with a clear 422 instead of a database error.
class UserSignup(BaseModel):
    username: str = Field(min_length=3, max_length=50, pattern=r"^[A-Za-z0-9_.-]+$")
    email: EmailStr = Field(max_length=100)
    phone: str = Field(min_length=3, max_length=20)
    # bcrypt only reads the first 72 bytes of a password
    password: str = Field(min_length=8, max_length=72)

class UserLogin(BaseModel):
    username: str = Field(min_length=1, max_length=50)
    password: str = Field(min_length=1, max_length=72)

# --- Helpers ---
def create_access_token(user_id: int, username: str) -> str:
    """Generates a short-lived, scope-enriched JWT for edge authentication."""
    payload = {
        "user_id": user_id,
        "username": username,
        "role": "customer",
        "scope": ["account:read", "transfer:create"],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=15),
        "iat": datetime.datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def _delete_user(user_id: int) -> bool:
    """Removes a user whose grant was refused. Returns False if the user can no
    longer be deleted because a transaction already references it (the grant
    did land after all)."""
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            conn.commit()
        return True
    except pymysql.err.IntegrityError:
        conn.rollback()
        return False
    finally:
        conn.close()

# --- Routes ---
@app.get("/health/liveness")
def liveness(): return {"status": "alive"}

@app.get("/health/readiness")
def readiness():
    try:
        conn = db_pool.connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        return {"status": "ready", "database": "connected"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database unready")

@app.post("/signup", status_code=status.HTTP_201_CREATED)
async def signup(user: UserSignup):
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            conn.begin()
            salt = bcrypt.gensalt()
            hashed = bcrypt.hashpw(user.password.encode('utf-8'), salt).decode('utf-8')

            cursor.execute(
                "INSERT INTO users (username, email, phone, password_hash) VALUES (%s, %s, %s, %s)",
                (user.username, user.email, user.phone, hashed)
            )
            new_user_id = cursor.lastrowid
            cursor.execute("INSERT INTO accounts (user_id, balance) VALUES (%s, 0.00)", (new_user_id,))
            conn.commit()
    except pymysql.err.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Username or Email already registered")
    except Exception:
        conn.rollback()
        # The database error goes to the log, not to the client.
        logger.exception("Signup failed")
        raise HTTPException(status_code=500, detail="Signup failed. Please try again.")
    finally:
        conn.close()

    # Welcome grant (connection safely closed before the network call)
    grant_status = await request_welcome_grant(new_user_id)

    if grant_status == 200:
        return {"message": "Account created successfully with $1,000 welcome grant."}

    if grant_status in (None, 409):
        # None: network error or timeout, outcome unknown.
        # 409:  the grant is already in flight under the same idempotency key.
        # Either way the identity is kept and the retry task settles the grant.
        logger.warning("Welcome grant pending for user_id=%s (transaction-service status=%s)", new_user_id, grant_status)
        return {"message": "Account created successfully. Welcome grant is pending settlement."}

    # Terminal error from transaction service (e.g., depleted treasury)
    logger.error("Welcome grant refused for user_id=%s (HTTP %s); rolling back signup", new_user_id, grant_status)
    deleted = await asyncio.to_thread(_delete_user, new_user_id)
    if not deleted:
        return {"message": "Account created successfully with $1,000 welcome grant."}
    raise HTTPException(status_code=500, detail="Treasury onboarding grant failed. Signup aborted.")

@app.post("/login")
def login(user: UserLogin):
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (user.username,))
            record = cursor.fetchone()
    finally:
        conn.close()

    if not record or not bcrypt.checkpw(user.password.encode('utf-8'), record['password_hash'].encode('utf-8')):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")

    token = create_access_token(record["id"], record["username"])
    return {"access_token": token, "token_type": "bearer", "username": record["username"], "user_id": record["id"]}
