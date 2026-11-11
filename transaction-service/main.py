from nexora_common import (
    configure_logging,
    current_correlation_id,
    install_request_logging,
    outbound_headers,
    prepare_multiprocess_metrics,
    warn_if_default_secret,
)

prepare_multiprocess_metrics()  # must run before prometheus_client is imported

from fastapi import FastAPI, HTTPException, Header, status
from pydantic import BaseModel, Field, condecimal
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from dbutils.pooled_db import PooledDB
from contextlib import asynccontextmanager
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional
import pymysql
import os
import httpx
import json
import secrets
import asyncio
import threading
import uuid

logger = configure_logging("transaction-service")

FRAUD_SVC_URL = os.getenv("FRAUD_SVC_URL", "http://fraud-service:8000")
_DEV_INTERNAL_SECRET = "nexora-internal-secret-key-123"
INTERNAL_SERVICE_SECRET = os.getenv("INTERNAL_SERVICE_SECRET", _DEV_INTERNAL_SECRET)
warn_if_default_secret(logger, "INTERNAL_SERVICE_SECRET", INTERNAL_SERVICE_SECRET, _DEV_INTERNAL_SECRET)

# A PROCESSING lease older than this is considered abandoned (its worker crashed)
# and may be taken over by a retry of the same idempotency key.
STALE_LEASE_SECONDS = 10

# -----------------------------------------------------------------------------
# Kafka / Outbox configuration
# Kafka is optional: if KAFKA_BOOTSTRAP_SERVERS is not set (e.g. prod today),
# no outbox rows are written and no publisher is started.
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_ENABLED = bool(KAFKA_BOOTSTRAP_SERVERS)
TRANSACTION_EVENTS_TOPIC = os.getenv("TRANSACTION_EVENTS_TOPIC", "transactions.events")
OUTBOX_POLL_INTERVAL = float(os.getenv("OUTBOX_POLL_INTERVAL_SECONDS", "1.0"))
OUTBOX_BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", "100"))

OUTBOX_PUBLISHED = Counter("outbox_events_published_total", "Outbox events delivered to Kafka", ["topic"])
OUTBOX_FAILURES = Counter("outbox_publish_failures_total", "Outbox publish attempts that failed and will be retried")
FRAUD_DECISIONS = Counter("transfer_fraud_decisions_total", "Fraud decisions received for transfers", ["threat_level"])

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

# =============================================================================
# OUTBOX PUBLISHER (background worker: outbox table -> Kafka)
# =============================================================================
def _build_producer():
    # Imported lazily so the service still starts if Kafka is disabled.
    from confluent_kafka import Producer

    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": protocol,
        "enable.idempotence": True,   # no duplicates from producer-side retries
        "acks": "all",                # wait for all in-sync replicas
        "client.id": "transaction-service-outbox",
    }
    # Staging/prod use SASL_SSL + SCRAM. Local docker-compose uses PLAINTEXT.
    if protocol.startswith("SASL"):
        conf["sasl.mechanisms"] = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        conf["sasl.username"] = os.getenv("KAFKA_USERNAME")
        conf["sasl.password"] = os.getenv("KAFKA_PASSWORD")
    if protocol.endswith("SSL") and os.getenv("KAFKA_CA_FILE"):
        conf["ssl.ca.location"] = os.getenv("KAFKA_CA_FILE")
    return Producer(conf)


def _publish_outbox_batch(producer) -> int:
    """
    Reads unpublished outbox rows, sends them to Kafka, and marks them published.
    FOR UPDATE SKIP LOCKED lets multiple transaction-service replicas run this
    at the same time without picking the same rows.
    Delivery is at-least-once: if we crash after Kafka acks but before commit,
    the rows are sent again (consumers de-duplicate on event_id).
    """
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            conn.begin()
            cursor.execute(
                """
                SELECT id, event_id, topic, event_key, payload
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (OUTBOX_BATCH_SIZE,)
            )
            rows = cursor.fetchall()
            if not rows:
                conn.commit()
                return 0

            failures = []

            def on_delivery(err, msg):
                if err is not None:
                    failures.append(err)

            for row in rows:
                payload = row["payload"]
                if not isinstance(payload, (str, bytes)):
                    payload = json.dumps(payload)
                producer.produce(
                    topic=row["topic"],
                    key=str(row["event_key"]),
                    value=payload,
                    headers=[("event_id", str(row["event_id"]))],
                    on_delivery=on_delivery,
                )
                producer.poll(0)

            not_sent = producer.flush(10)
            if not_sent > 0 or failures:
                conn.rollback()
                raise RuntimeError(
                    f"Kafka delivery failed (undelivered={not_sent}, errors={failures[:3]})"
                )

            ids = [r["id"] for r in rows]
            placeholders = ",".join(["%s"] * len(ids))
            cursor.execute(
                f"UPDATE outbox SET published_at = NOW() WHERE id IN ({placeholders})",
                ids
            )
            conn.commit()
            for row in rows:
                OUTBOX_PUBLISHED.labels(topic=row["topic"]).inc()
            return len(rows)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _outbox_publisher_loop(stop_event: threading.Event) -> None:
    try:
        producer = _build_producer()
    except Exception:
        logger.exception("Outbox publisher could not start (Kafka client setup failed)")
        return

    logger.info("Outbox publisher started (topic=%s)", TRANSACTION_EVENTS_TOPIC)
    while not stop_event.is_set():
        try:
            sent = _publish_outbox_batch(producer)
            if sent:
                logger.info("Outbox publisher sent %d event(s) to Kafka", sent)
        except Exception:
            # Rows stay unpublished (published_at IS NULL) and are retried.
            OUTBOX_FAILURES.inc()
            logger.exception("Outbox publish failed; will retry")
            stop_event.wait(5)
            continue
        if sent == 0:
            stop_event.wait(OUTBOX_POLL_INTERVAL)

    try:
        producer.flush(5)
    except Exception:
        pass
    logger.info("Outbox publisher stopped")


def _enqueue_transfer_event(
    cursor,
    transaction_id: int,
    sender_id: int,
    receiver_id: int,
    amount: Decimal,
    idempotency_key: str,
    correlation_id: str = None,
    fraud: dict = None,
) -> None:
    """
    Writes the event to the outbox using the SAME cursor/connection as the transfer,
    so the event is committed or rolled back together with the money movement.
    """
    if not KAFKA_ENABLED:
        return

    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "event_type": "transfer.completed",
        "transaction_id": transaction_id,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "amount": str(amount),
        "idempotency_key": idempotency_key,
        "correlation_id": correlation_id,
        # The synchronous fraud decision: fraud-service turns medium/high into alerts.
        "fraud": fraud or {"threat_level": "low", "score": 0, "reasons": []},
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    cursor.execute(
        "INSERT INTO outbox (event_id, topic, event_key, payload) VALUES (%s, %s, %s, %s)",
        # Keyed by sender so one customer's events stay ordered within a partition.
        (event_id, TRANSACTION_EVENTS_TOPIC, str(sender_id), json.dumps(event))
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=2.0)

    stop_event = threading.Event()
    publisher_thread = None
    if KAFKA_ENABLED:
        publisher_thread = threading.Thread(
            target=_outbox_publisher_loop,
            args=(stop_event,),
            name="outbox-publisher",
            daemon=True,
        )
        publisher_thread.start()
    else:
        logger.info("KAFKA_BOOTSTRAP_SERVERS not set: outbox publishing disabled")

    yield

    stop_event.set()
    if publisher_thread is not None:
        publisher_thread.join(timeout=15)
    await http_client.aclose()
    db_pool.close()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)
install_request_logging(app, logger)

# The idempotency_key column is VARCHAR(64); longer keys are rejected up front.
IDEMPOTENCY_KEY_MAX_LENGTH = 64

class TransferPayload(BaseModel):
    receiver_username: str = Field(min_length=1, max_length=50)
    amount: condecimal(gt=Decimal('0.00'), max_digits=15, decimal_places=2)

class SystemTransferPayload(BaseModel):
    sender_id: int
    receiver_id: int
    amount: condecimal(gt=Decimal('0.00'), max_digits=15, decimal_places=2)
    idempotency_key: str = Field(min_length=1, max_length=IDEMPOTENCY_KEY_MAX_LENGTH)

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
    raise HTTPException(status_code=status_code, detail=message)

# =============================================================================
# IDEMPOTENCY LEASE (shared by user transfers and system transfers)
# =============================================================================
def _sync_claim_lease(sender_id: int, idempotency_key: str) -> dict:
    """
    Claims the idempotency key for this request.
      {"status": "claimed", "version": N}  - we own the lease (N is the fencing token)
      {"status": "cached", "response": {}} - already completed, replay the stored answer
      {"status": "conflict"}               - another request holds a fresh lease
    A PROCESSING lease older than STALE_LEASE_SECONDS belonged to a worker that
    died mid-flight; it is taken over by bumping lease_version, which also makes
    the dead worker's final UPDATE (WHERE lease_version = old) match nothing.
    """
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
                      AND updated_at < NOW() - INTERVAL %s SECOND
                    """,
                    (sender_id, idempotency_key, STALE_LEASE_SECONDS)
                )
                conn.commit()
                if cursor.rowcount != 1:
                    return {"status": "conflict"}

                cursor.execute("SELECT lease_version FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s", (sender_id, idempotency_key))
                return {"status": "claimed", "version": cursor.fetchone()["lease_version"]}
    finally:
        conn.close()

def _sync_release_lease(sender_id: int, idempotency_key: str, lease_version: int) -> None:
    """Drops a lease we own so the client can retry the same key immediately."""
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM idempotency_records WHERE user_id = %s AND idempotency_key = %s AND lease_version = %s",
                (sender_id, idempotency_key, lease_version)
            )
            conn.commit()
    finally:
        conn.close()

# =============================================================================
# 1. INTERNAL SYSTEM TRANSFER (For Treasury Onboarding Grants)
# =============================================================================
def _sync_system_transfer(sender_id: int, receiver_id: int, amount: Decimal, key: str) -> dict:
    # Phase 1: Claim the idempotency key (same lease logic as user transfers,
    # including takeover of a lease abandoned by a crashed worker).
    claim = _sync_claim_lease(sender_id, key)
    if claim["status"] == "cached":
        cached = claim["response"]
        if cached.get("status") == "declined":
            raise HTTPException(status_code=500, detail=cached.get("message"))
        return cached
    if claim["status"] == "conflict":
        raise HTTPException(status_code=409, detail="System transfer in flight")
    lease_version = claim["version"]

    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            # Phase 3: Immediate Execution with Row Locks
            conn.begin()
            first_lock_id, second_lock_id = sorted([sender_id, receiver_id])
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (first_lock_id,))
            cursor.execute("SELECT user_id, balance FROM accounts WHERE user_id = %s FOR UPDATE", (second_lock_id,))

            cursor.execute("SELECT balance FROM accounts WHERE user_id = %s", (sender_id,))
            sender_acc = cursor.fetchone()
            if not sender_acc or Decimal(str(sender_acc["balance"])) < amount:
                conn.rollback()
                record_terminal_decline(sender_id, key, "Treasury reserve depleted.", 500, lease_version=lease_version)

            cursor.execute("UPDATE accounts SET balance = balance - %s WHERE user_id = %s", (amount, sender_id))
            cursor.execute("UPDATE accounts SET balance = balance + %s WHERE user_id = %s", (amount, receiver_id))

            cursor.execute(
                "INSERT INTO transactions (idempotency_key, sender_id, receiver_id, amount) VALUES (%s, %s, %s, %s)",
                (key, sender_id, receiver_id, amount)
            )

            success_payload = {
                "status": "success",
                "message": f"${amount} system transfer executed to User {receiver_id}.",
                "idempotency_key": key
            }
            cursor.execute(
                """
                UPDATE idempotency_records
                SET status = 'COMPLETED', response_body = %s
                WHERE user_id = %s AND idempotency_key = %s AND lease_version = %s
                """,
                (json.dumps(success_payload), sender_id, key, lease_version)
            )

            # Fencing: if our lease was taken over while we were working, undo everything.
            if cursor.rowcount != 1:
                conn.rollback()
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Fencing token mismatch")

            conn.commit()
            return success_payload
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        # The database error goes to the log, not to the caller.
        logger.exception("System transfer failed")
        raise HTTPException(status_code=500, detail="System transfer failed")
    finally:
        conn.close()

@app.post("/internal/system-transfer")
async def execute_system_transfer(
    payload: SystemTransferPayload,
    x_internal_key: str = Header(..., alias="X-Internal-Service-Key")
):
    if not secrets.compare_digest(x_internal_key, INTERNAL_SERVICE_SECRET):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized internal service call")

    if payload.sender_id != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="System transfer key is restricted strictly to Treasury Reserve debits"
        )

    return await asyncio.to_thread(
        _sync_system_transfer,
        payload.sender_id,
        payload.receiver_id,
        payload.amount,
        payload.idempotency_key
    )

# =============================================================================
# 2. USER-INITIATED TRANSFER (For Customer Peer-to-Peer Transfers)
# =============================================================================
def _sync_fraud_facts(sender_id: int, receiver_username: str, amount: Decimal) -> dict:
    """
    Reads the facts the fraud rules need. fraud-service is stateless and has no
    database, so everything it scores on is collected here and sent with the scan.
    Read-only, no locks; the authoritative balance check still happens in Phase 3.
    """
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.balance, TIMESTAMPDIFF(SECOND, u.created_at, NOW()) AS age_seconds
                FROM users u JOIN accounts a ON a.user_id = u.id
                WHERE u.id = %s
                """,
                (sender_id,)
            )
            sender = cursor.fetchone() or {}

            cursor.execute(
                "SELECT COUNT(*) AS tx_count FROM transactions WHERE sender_id = %s AND timestamp >= NOW() - INTERVAL 60 SECOND",
                (sender_id,)
            )
            tx_count_1m = cursor.fetchone()["tx_count"]

            cursor.execute(
                "SELECT COALESCE(SUM(amount), 0) AS total FROM transactions WHERE sender_id = %s AND timestamp >= NOW() - INTERVAL 24 HOUR",
                (sender_id,)
            )
            amount_24h = cursor.fetchone()["total"]

            cursor.execute("SELECT id FROM users WHERE username = %s", (receiver_username,))
            receiver = cursor.fetchone()
            receiver_id = receiver["id"] if receiver else None

            first_time_receiver = False
            if receiver_id is not None:
                cursor.execute(
                    "SELECT 1 FROM transactions WHERE sender_id = %s AND receiver_id = %s LIMIT 1",
                    (sender_id, receiver_id)
                )
                first_time_receiver = cursor.fetchone() is None
        conn.commit()  # end the read snapshot before the connection goes back to the pool
    finally:
        conn.close()

    balance = sender.get("balance")
    age_seconds = sender.get("age_seconds")
    return {
        "amount": str(amount),
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "sender_balance": str(balance) if balance is not None else None,
        "sender_tx_count_1m": int(tx_count_1m),
        "sender_amount_24h": str(amount_24h),
        "sender_account_age_seconds": max(int(age_seconds), 0) if age_seconds is not None else None,
        "first_time_receiver": first_time_receiver,
    }

def _sync_db_phase(
    sender_id: int,
    receiver_username: str,
    amount: Decimal,
    idempotency_key: str,
    acquired_lease_version: int,
    correlation_id: str = None,
    fraud: dict = None
) -> dict:
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
            transaction_id = cursor.lastrowid

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

            # Outbox: written in the SAME DB transaction as the transfer.
            # If anything above fails and we roll back, no event is recorded either.
            _enqueue_transfer_event(
                cursor,
                transaction_id,
                sender_id,
                receiver_id,
                amount,
                idempotency_key,
                correlation_id,
                fraud,
            )

            conn.commit()
            return success_payload
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        # The database error goes to the log, not to the client.
        logger.exception("Transfer failed")
        raise HTTPException(status_code=500, detail="Transfer could not be completed. Please try again.")
    finally:
        conn.close()

async def _request_fraud_decision(facts: dict) -> httpx.Response:
    """Asks fraud-service to score the transfer."""
    response = await http_client.post(f"{FRAUD_SVC_URL}/api/scan", json=facts, headers=outbound_headers())
    if response.status_code in (404, 405):
        # A fraud-service pod from the previous image (GET-only contract) answered.
        # Only possible in the middle of a rolling deploy.
        response = await http_client.get(f"{FRAUD_SVC_URL}/api/scan", headers=outbound_headers())
    return response

@app.post("/transfer")
async def execute_transfer(
    payload: TransferPayload,
    x_user_id: int = Header(..., alias="X-User-Id"),
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=IDEMPOTENCY_KEY_MAX_LENGTH),
    x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-ID")
):
    sender_id = x_user_id
    amount: Decimal = payload.amount
    correlation_id = x_correlation_id or current_correlation_id()

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

    # --- Phase 2: Async Non-Blocking Fraud Check (no DB connection held) ---
    try:
        facts = await asyncio.to_thread(_sync_fraud_facts, sender_id, payload.receiver_username, amount)
        fraud_res = await _request_fraud_decision(facts)
    except (httpx.RequestError, pymysql.MySQLError):
        # Nothing has been decided yet: drop the lease so the same key can be retried.
        logger.exception("Fraud check could not be completed")
        await asyncio.to_thread(_sync_release_lease, sender_id, idempotency_key, acquired_lease_version)
        raise HTTPException(status_code=503, detail="Fraud engine unreachable. Transfer aborted for safety.")

    decision = fraud_res.json() if fraud_res.status_code == 200 else {}
    threat_level = decision.get("threat_level")
    FRAUD_DECISIONS.labels(threat_level=threat_level or "error").inc()
    # low = allow, medium = allow and flag (alert via Kafka), anything else = decline.
    if threat_level not in ("low", "medium"):
        logger.warning("Transfer declined by fraud check: threat_level=%s score=%s reasons=%s",
                       threat_level, decision.get("score"), decision.get("reasons"))
        await asyncio.to_thread(
            record_terminal_decline,
            sender_id, idempotency_key, "Transaction declined by Fraud Detection Engine.", 403, acquired_lease_version
        )

    fraud = {
        "threat_level": threat_level,
        "score": decision.get("score", 0),
        "reasons": decision.get("reasons", []),
    }

    # --- Phase 3: Threadpool Offloaded Financial Execution ---
    return await asyncio.to_thread(
        _sync_db_phase,
        sender_id,
        payload.receiver_username,
        amount,
        idempotency_key,
        acquired_lease_version,
        correlation_id,
        fraud
    )
