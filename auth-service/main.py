from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator
from dbutils.pooled_db import PooledDB
from contextlib import asynccontextmanager
import pymysql
import os
import bcrypt
import jwt
import datetime
import httpx

JWT_SECRET = os.getenv("JWT_SECRET", "nexora-super-secret-key-change-in-prod")
JWT_ALGORITHM = "HS256"
TRANSACTION_SVC_URL = os.getenv("TRANSACTION_SVC_URL", "http://transaction-service:8000")
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", "nexora-internal-secret-key-123")

db_pool = PooledDB(
    creator=pymysql,
    maxconnections=10,
    mincached=0,  # Lazy initialization
    maxcached=2,
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "root"),
    database=os.getenv("DB_NAME", "nexora_bank"),
    cursorclass=pymysql.cursors.DictCursor
)

http_client: httpx.AsyncClient = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    yield
    await http_client.aclose()
    db_pool.close()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

class UserSignup(BaseModel):
    username: str
    email: str
    phone: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

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
            
            # 1. Atomic Provisioning: Create Identity Record
            cursor.execute(
                "INSERT INTO users (username, email, phone, password_hash) VALUES (%s, %s, %s, %s)", 
                (user.username, user.email, user.phone, hashed)
            )
            new_user_id = cursor.lastrowid
            
            # 2. Open Checking Account ($0.00 base balance)
            cursor.execute("INSERT INTO accounts (user_id, balance) VALUES (%s, 0.00)", (new_user_id,))
            conn.commit()

    except pymysql.err.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Username or Email already registered")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    # 3. DETERMINISTIC IDEMPOTENCY KEY (No Random UUIDs)
    # Enables safe, infinite retries without double-crediting or ambiguous delete hazards
    deterministic_grant_key = f"grant-user-{new_user_id}"
    try:
        grant_res = await http_client.post(
            f"{TRANSACTION_SVC_URL}/internal/system-transfer",
            headers={"X-Internal-Service-Key": INTERNAL_SERVICE_SECRET},
            json={
                "sender_id": 1,  # Nexora Treasury Reserve
                "receiver_id": new_user_id,
                "amount": "1000.00",
                "idempotency_key": deterministic_grant_key
            }
        )
        if grant_res.status_code == 200:
            return {"message": "Account created successfully with $1,000 welcome grant."}
    except httpx.RequestError:
        pass  # On network timeout/blip: DO NOT delete local state. The grant is safely retryable!

    return {"message": "Account created successfully. Welcome grant is pending settlement."}

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

    # Role & Scope-Enriched 15-Minute Access Token
    payload = {
        "user_id": record["id"],
        "username": record["username"],
        "role": "customer",
        "scope": ["account:read", "transfer:create"],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=15),
        "iat": datetime.datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return {"access_token": token, "token_type": "bearer", "username": record["username"], "user_id": record["id"]}