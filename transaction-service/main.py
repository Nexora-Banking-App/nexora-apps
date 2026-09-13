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
import asyncio

FRAUD_SVC_URL = os.getenv("FRAUD_SVC_URL", "http://fraud-service:8000")
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", "nexora-internal-secret-key-123")

db_pool = PooledDB(
    creator=pymysql,
    maxconnections=20,
    mincached=0,
    maxcached=5,
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "root"),
    database=os.getenv("DB_NAME", "nexora_bank"),
    cursorclass=pymysql.cursors.DictCursor
)

http_client: httpx.AsyncClient

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

def record_terminal_decline(user_id: int, key: str, message: str, status_code: int, lease_version: int = None) -> None:
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
    # Explicitly raises exception so execution NEVER falls through!
    raise HTTPException(status_code=status_code, detail=message)

# =============================================================================
# THREAD-OFFLOADED SYNCHRONOUS DB WORKERS
# =============================================================================
def _sync_claim_lease(sender_id: int, idempotency_key: str) -> dict:
    """Threadpool worker: Claims the lease atomically without blocking asyncio event loop."""
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO idempotency_records (user_id, idempotency_key, status, lease_version) VALUES (%s, %s, 'PROCESSING', 1)",
                    (sender_id, idempotency_key)
                )
                conn.commit()
                return {"status": "claimed", "version": 1}
            except pymysql.err.IntegrityError:
                cursor.execute(
                    "SELECT status, response_body FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s",
                    (sender_id, idempotency_key)
                )
                existing = cursor.fetchone()
                if existing and existing["status"] == "COMPLETED" and existing["response_body"]:
                    return {"status": "cached", "response": json.loads(existing["response_body"])}

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
                    return {"status": "conflict"}

                cursor.execute("SELECT lease_version FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s", (sender_id, idempotency_key))
                return {"status": "claimed", "version": cursor.fetchone()["lease_version"]}
    finally:
        conn.close()

def _sync_db_phase(sender_id: int, receiver_username: str, amount: Decimal, idempotency_key: str, acquired_lease_version: int) -> dict:
    """Threadpool worker: Executes row-locks, balance mutation, and audit log."""
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            conn.begin()

            cursor.execute("SELECT id FROM users WHERE username = %s", (receiver_username,))
            receiver = cursor.fetchone()
            if not receiver:
                conn.rollback()
                record_terminal_decline(sender_id, idempotency_key, "Recipient username not found.", 404, acquired_lease_version)

            receiver_id = receiver["id"]
            if sender_id == receiver_id:
                conn.rollback()
                record_terminal_decline(sender_id, idempotency_key, "Cannot transfer funds to yourself.", 400, acquired_lease_version)

            first_lock_id, second_lock_id = sorted([sender_id, receiver_id])
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (first_lock_id,))
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (second_lock_id,))

            cursor.execute("SELECT balance FROM accounts WHERE user_id = %s", (sender_id,))
            sender_acc = cursor.fetchone()
            if not sender_acc or Decimal(str(sender_acc["balance"])) < amount:
                conn.rollback()
                record_terminal_decline(sender_id, idempotency_key, "Insufficient funds.", 400, acquired_lease_version)

            cursor.execute("UPDATE accounts SET balance = balance - %s WHERE user_id = %s", (amount, sender_id))
            cursor.execute("UPDATE accounts SET balance = balance + %s WHERE user_id = %s", (amount, receiver_id))

            cursor.execute(
                "INSERT INTO transactions (idempotency_key, sender_id, receiver_id, amount) VALUES (%s, %s, %s, %s)",
                (idempotency_key, sender_id, receiver_id, amount)
            )

            success_payload = {
                "status": "success",
                "message": f"${amount} transferred to {receiver_username}.",
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
                conn.rollback()
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Fencing token mismatch")

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
# MAIN ASYNC ROUTE (100% Non-Blocking Event Loop)
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

    # --- Phase 1: Threadpool Offloaded Lease Claim ---
    claim = await asyncio.to_thread(_sync_claim_lease, sender_id, idempotency_key)
    
    if claim["status"] == "cached":
        res = claim["response"]
        if res.get("status") == "declined":
            raise HTTPException(status_code=400, detail=res.get("message"))
        return res
    elif claim["status"] == "conflict":
        raise HTTPException(status_code=409, detail="Transfer currently in flight. Please wait.")
    
    acquired_lease_version = claim["version"]

    # --- Phase 2: Async Non-Blocking Fraud Check ---
    try:
        fraud_res = await http_client.get(
            f"{FRAUD_SVC_URL}/api/scan",
            headers={"X-Correlation-ID": x_correlation_id or ""}
        )
        if fraud_res.status_code != 200 or fraud_res.json().get("threat_level") != "low":
            record_terminal_decline(sender_id, idempotency_key, "Transaction declined by Fraud Detection Engine.", 403, acquired_lease_version)
    except httpx.RequestError:
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

    # --- Phase 3: Threadpool Offloaded Financial Execution ---
    return await asyncio.to_thread(
        _sync_db_phase, 
        sender_id, 
        payload.receiver_username, 
        amount, 
        idempotency_key, 
        acquired_lease_version
    )