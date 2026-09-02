from fastapi import FastAPI, HTTPException, Header, status
from pydantic import BaseModel, condecimal
from prometheus_fastapi_instrumentator import Instrumentator
from dbutils.pooled_db import PooledDB
from contextlib import asynccontextmanager
from decimal import Decimal
import pymysql
import os
import httpx
import json
import secrets

FRAUD_SVC_URL = os.getenv("FRAUD_SVC_URL", "http://fraud-service:8000")
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", "nexora-internal-secret-key-123")

db_pool = PooledDB(
    creator=pymysql,
    maxconnections=20,
    mincached=0,  # Lazy initialization
    maxcached=5,
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
    http_client = httpx.AsyncClient(timeout=2.0)
    yield
    await http_client.aclose()
    db_pool.close()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

class TransferPayload(BaseModel):
    receiver_username: str
    amount: condecimal(gt=Decimal('0.00'), max_digits=15, decimal_places=2)

class SystemTransferPayload(BaseModel):
    sender_id: int
    receiver_id: int
    amount: condecimal(gt=Decimal('0.00'), max_digits=15, decimal_places=2)
    idempotency_key: str

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
        raise HTTPException(status_code=503, detail="Database pool unready")

def record_terminal_decline(user_id: int, key: str, message: str, status_code: int, lease_version: int = None):
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            decline_payload = {"status": "declined", "message": message}
            if lease_version is not None:
                cursor.execute(
                    """
                    UPDATE idempotency_records 
                    SET status = 'COMPLETED', response_body = %s 
                    WHERE user_id = %s AND idempotency_key = %s AND lease_version = %s
                    """,
                    (json.dumps(decline_payload), user_id, key, lease_version)
                )
            else:
                cursor.execute(
                    "UPDATE idempotency_records SET status = 'COMPLETED', response_body = %s WHERE user_id = %s AND idempotency_key = %s",
                    (json.dumps(decline_payload), user_id, key)
                )
            conn.commit()
    finally:
        conn.close()
    raise HTTPException(status_code=status_code, detail=message)

# =============================================================================
# 1. INTERNAL SYSTEM TRANSFER (Scoped Strictly to Treasury Reserve with Constant-Time Check)
# =============================================================================
@app.post("/internal/system-transfer")
async def execute_system_transfer(
    payload: SystemTransferPayload,
    x_internal_key: str = Header(..., alias="X-Internal-Service-Key")
):
    # Constant-time comparison to prevent timing attacks
    if not secrets.compare_digest(x_internal_key, INTERNAL_SERVICE_SECRET):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized internal service call")

    # Least-Privilege Scoping: Restrict system transfers strictly to Treasury Reserve (User ID 1)
    if payload.sender_id != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Forbidden: System transfer key is restricted strictly to Treasury Reserve debits (sender_id=1)"
        )

    sender_id = payload.sender_id
    receiver_id = payload.receiver_id
    amount: Decimal = payload.amount
    key = payload.idempotency_key

    # Phase 1: Atomic Idempotency Claim
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO idempotency_records (user_id, idempotency_key, status, lease_version) VALUES (%s, %s, 'PROCESSING', 1)",
                    (sender_id, key)
                )
                conn.commit()
            except pymysql.err.IntegrityError:
                cursor.execute(
                    "SELECT status, response_body FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s",
                    (sender_id, key)
                )
                existing = cursor.fetchone()
                if existing and existing["status"] == "COMPLETED":
                    return json.loads(existing["response_body"])
                raise HTTPException(status_code=409, detail="System transfer currently in flight.")
    finally:
        conn.close()

    # Phase 3: Immediate Execution with Row Locks & Version Gating
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            conn.begin()

            first_lock_id, second_lock_id = sorted([sender_id, receiver_id])
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (first_lock_id,))
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (second_lock_id,))

            cursor.execute("SELECT balance FROM accounts WHERE user_id = %s", (sender_id,))
            sender_acc = cursor.fetchone()
            if not sender_acc or Decimal(str(sender_acc["balance"])) < amount:
                conn.rollback()
                record_terminal_decline(sender_id, key, "Treasury reserve depleted.", 500, lease_version=1)

            cursor.execute("UPDATE accounts SET balance = balance - %s WHERE user_id = %s", (amount, sender_id))
            cursor.execute("UPDATE accounts SET balance = balance + %s WHERE user_id = %s", (amount, receiver_id))

            cursor.execute(
                "INSERT INTO transactions (idempotency_key, sender_id, receiver_id, amount) VALUES (%s, %s, %s, %s)",
                (key, sender_id, receiver_id, amount)
            )

            success_payload = {
                "status": "success",
                "message": f"${amount} system transfer executed from User {sender_id} to User {receiver_id}.",
                "idempotency_key": key
            }
            cursor.execute(
                """
                UPDATE idempotency_records 
                SET status = 'COMPLETED', response_body = %s 
                WHERE user_id = %s AND idempotency_key = %s AND lease_version = 1
                """,
                (json.dumps(success_payload), sender_id, key)
            )

            conn.commit()
            return success_payload
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# =============================================================================
# 2. USER-INITIATED TRANSFER (Fencing-Token Guarded Pipeline)
# =============================================================================
@app.post("/transfer")
async def execute_transfer(
    payload: TransferPayload,
    x_user_id: int = Header(..., alias="X-User-Id"),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    x_correlation_id: str = Header(None, alias="X-Correlation-ID")
):
    sender_id = x_user_id
    amount: Decimal = payload.amount
    acquired_lease_version = 1

    # =========================================================================
    # PHASE 1: ATOMIC LEASE CLAIM & FENCING TOKEN MINTING (<1ms)
    # =========================================================================
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            try:
                # Initial Lease Claim: lease_version = 1
                cursor.execute(
                    "INSERT INTO idempotency_records (user_id, idempotency_key, status, lease_version) VALUES (%s, %s, 'PROCESSING', 1)",
                    (sender_id, idempotency_key)
                )
                conn.commit()
                acquired_lease_version = 1
            except pymysql.err.IntegrityError:
                # Key already exists: check if completed
                cursor.execute(
                    "SELECT status, response_body FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s",
                    (sender_id, idempotency_key)
                )
                existing = cursor.fetchone()

                if existing and existing["status"] == "COMPLETED" and existing["response_body"]:
                    cached_res = json.loads(existing["response_body"])
                    if cached_res.get("status") == "declined":
                        raise HTTPException(status_code=400, detail=cached_res.get("message"))
                    return cached_res

                # Atomic CAS Staleness Recovery with Monotonic Fencing Token Increment
                cursor.execute(
                    """
                    UPDATE idempotency_records 
                    SET updated_at = NOW(), status = 'PROCESSING', lease_version = lease_version + 1
                    WHERE user_id = %s AND idempotency_key = %s 
                      AND status = 'PROCESSING' 
                      AND updated_at < NOW() - INTERVAL 10 SECOND
                    """,
                    (sender_id, idempotency_key)
                )
                conn.commit()

                if cursor.rowcount != 1:
                    raise HTTPException(status_code=409, detail="Transfer currently in flight. Please wait.")

                # Read the newly minted fencing token
                cursor.execute(
                    "SELECT lease_version FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s",
                    (sender_id, idempotency_key)
                )
                row = cursor.fetchone()
                acquired_lease_version = row["lease_version"]
    finally:
        conn.close()

    # =========================================================================
    # PHASE 2: SYNCHRONOUS FRAUD CHECK (0 DB Connections Held!)
    # =========================================================================
    try:
        fraud_res = await http_client.get(
            f"{FRAUD_SVC_URL}/api/scan",
            headers={"X-Correlation-ID": x_correlation_id or ""}
        )
        if fraud_res.status_code != 200 or fraud_res.json().get("threat_level") != "low":
            record_terminal_decline(sender_id, idempotency_key, "Transaction declined by Fraud Detection Engine.", 403, acquired_lease_version)
    except httpx.RequestError:
        # Transient System Failure: Delete lease only if our fencing token is still active
        conn = db_pool.connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s AND lease_version = %s",
                    (sender_id, idempotency_key, acquired_lease_version)
                )
                conn.commit()
        finally:
            conn.close()
        raise HTTPException(status_code=503, detail="Fraud engine unreachable. Transfer aborted for safety.")

    # =========================================================================
    # PHASE 3: FINANCIAL EXECUTION WITH FENCING-TOKEN VALIDATION (<5ms)
    # =========================================================================
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            conn.begin()

            # 1. Resolve Recipient
            cursor.execute("SELECT id FROM users WHERE username = %s", (payload.receiver_username,))
            receiver = cursor.fetchone()
            if not receiver:
                conn.rollback()
                record_terminal_decline(sender_id, idempotency_key, "Recipient username not found.", 404, acquired_lease_version)

            receiver_id = receiver["id"]
            if sender_id == receiver_id:
                conn.rollback()
                record_terminal_decline(sender_id, idempotency_key, "Cannot transfer funds to yourself.", 400, acquired_lease_version)

            # 2. Deterministic Ascending Lock Acquisition
            first_lock_id, second_lock_id = sorted([sender_id, receiver_id])
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (first_lock_id,))
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (second_lock_id,))

            # 3. Validate Balance
            cursor.execute("SELECT balance FROM accounts WHERE user_id = %s", (sender_id,))
            sender_acc = cursor.fetchone()
            if not sender_acc or Decimal(str(sender_acc["balance"])) < amount:
                conn.rollback()
                record_terminal_decline(sender_id, idempotency_key, "Insufficient funds.", 400, acquired_lease_version)

            # 4. Mutate Balances
            cursor.execute("UPDATE accounts SET balance = balance - %s WHERE user_id = %s", (amount, sender_id))
            cursor.execute("UPDATE accounts SET balance = balance + %s WHERE user_id = %s", (amount, receiver_id))

            # 5. Insert Audit Record
            cursor.execute(
                "INSERT INTO transactions (idempotency_key, sender_id, receiver_id, amount) VALUES (%s, %s, %s, %s)",
                (idempotency_key, sender_id, receiver_id, amount)
            )

            # 6. FENCING TOKEN COMMIT GATE: Validate lease ownership has not been revoked!
            success_payload = {
                "status": "success",
                "message": f"${amount} transferred to {payload.receiver_username}.",
                "idempotency_key": idempotency_key,
                "fencing_token": acquired_lease_version
            }
            cursor.execute(
                """
                UPDATE idempotency_records 
                SET status = 'COMPLETED', response_body = %s 
                WHERE user_id = %s AND idempotency_key = %s AND lease_version = %s
                """,
                (json.dumps(success_payload), sender_id, idempotency_key, acquired_lease_version)
            )

            if cursor.rowcount != 1:
                # FENCING TOKEN VIOLATION: Worker stalled in Phase 2; a retry reclaimed the lease!
                conn.rollback()
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Fencing token mismatch: Lease expired and was reclaimed by a concurrent retry during execution."
                )

            conn.commit()
            return success_payload

    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Internal server error during transfer")
    finally:
        conn.close()