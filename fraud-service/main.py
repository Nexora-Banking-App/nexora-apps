from nexora_common import configure_logging, install_request_logging, prepare_multiprocess_metrics

prepare_multiprocess_metrics()  # must run before prometheus_client is imported

from collections import OrderedDict
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Optional
import json
import math
import os
import threading

from fastapi import FastAPI
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

from rules import TransferFacts, score_transfer

logger = configure_logging("fraud-service")

# -----------------------------------------------------------------------------
# Kafka consumer configuration
# Kafka is optional: if KAFKA_BOOTSTRAP_SERVERS is not set (e.g. prod today),
# the consumer is not started and only the synchronous /api/scan check runs.
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_ENABLED = bool(KAFKA_BOOTSTRAP_SERVERS)
# Must match the group the KafkaUser ACL allows (group "fraud-service").
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-service")
# Comma-separated. fraud-service is allowed to read transactions.events and auth.events.
KAFKA_TOPICS = [t.strip() for t in os.getenv("KAFKA_TOPICS", "transactions.events").split(",") if t.strip()]
FRAUD_ALERTS_TOPIC = os.getenv("FRAUD_ALERTS_TOPIC", "fraud.alerts")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "transactions.events.dlq")
DEDUPE_CACHE_SIZE = int(os.getenv("FRAUD_DEDUPE_CACHE_SIZE", "10000"))

# CPU work per scan. 0 = rule engine only. The Kubernetes manifests set a value
# so the HorizontalPodAutoscaler demo still has load to react to.
SIMULATED_WORK_ITERATIONS = int(os.getenv("FRAUD_SIMULATED_WORK_ITERATIONS", "0"))

SCANS = Counter("fraud_scans_total", "Synchronous fraud scans by resulting threat level", ["threat_level"])
RULE_HITS = Counter("fraud_rule_hits_total", "Fraud rules that fired", ["rule"])
EVENTS_CONSUMED = Counter("fraud_events_consumed_total", "Events consumed from Kafka by fraud-service", ["topic"])
EVENTS_DUPLICATE = Counter("fraud_events_duplicate_total", "Events skipped because their event_id was already processed", ["topic"])
EVENTS_MALFORMED = Counter("fraud_events_malformed_total", "Kafka messages that could not be processed and were sent to the DLQ", ["topic"])
ALERTS_PUBLISHED = Counter("fraud_alerts_published_total", "Alerts published to the fraud alerts topic", ["threat_level"])
KAFKA_PUBLISH_ERRORS = Counter("fraud_kafka_publish_errors_total", "Failed publishes to Kafka", ["topic"])


# =============================================================================
# Kafka consumer (transactions.events -> fraud.alerts / DLQ)
# =============================================================================
def _kafka_base_conf() -> dict:
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "security.protocol": protocol}
    if protocol.startswith("SASL"):
        conf["sasl.mechanisms"] = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        conf["sasl.username"] = os.getenv("KAFKA_USERNAME")
        conf["sasl.password"] = os.getenv("KAFKA_PASSWORD")
    if protocol.endswith("SSL") and os.getenv("KAFKA_CA_FILE"):
        conf["ssl.ca.location"] = os.getenv("KAFKA_CA_FILE")
    return conf


def _build_consumer():
    # Imported lazily so the service still starts if Kafka is disabled.
    from confluent_kafka import Consumer

    conf = _kafka_base_conf()
    conf.update({
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        # Offsets are committed only after an event has been handled.
        "enable.auto.commit": False,
        "client.id": "fraud-service-consumer",
    })
    return Consumer(conf)


def _build_producer():
    from confluent_kafka import Producer

    conf = _kafka_base_conf()
    conf.update({"enable.idempotence": True, "acks": "all", "client.id": "fraud-service-producer"})
    return Producer(conf)


class _SeenEvents:
    """Bounded memory of processed event_ids (delivery is at-least-once)."""

    def __init__(self, size: int) -> None:
        self._size = size
        self._seen: "OrderedDict[str, None]" = OrderedDict()

    def add(self, event_id: str) -> bool:
        """Return True if the id is new, False if it was seen before."""
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return False
        self._seen[event_id] = None
        if len(self._seen) > self._size:
            self._seen.popitem(last=False)
        return True


def _publish(producer, topic: str, key: Optional[str], value: bytes, headers: Optional[list] = None) -> bool:
    try:
        producer.produce(topic=topic, key=key, value=value, headers=headers or [])
        return producer.flush(10) == 0
    except Exception:
        logger.exception("Kafka publish to %s failed", topic)
        return False


def _handle_message(msg, producer, seen: _SeenEvents) -> None:
    topic = msg.topic()
    raw = msg.value()
    try:
        event = json.loads(raw)
        event_id = event["event_id"]
        if not isinstance(event_id, str):
            raise ValueError("event_id is not a string")
    except (ValueError, TypeError, KeyError) as exc:
        # Not valid JSON, or no event_id: park it so it never blocks the partition.
        EVENTS_MALFORMED.labels(topic=topic).inc()
        logger.warning("Sending unprocessable message to DLQ (topic=%s partition=%s offset=%s): %s",
                       topic, msg.partition(), msg.offset(), exc)
        headers = [("source_topic", topic.encode()), ("error", str(exc).encode()[:200])]
        if not _publish(producer, DLQ_TOPIC, None, raw or b"", headers):
            KAFKA_PUBLISH_ERRORS.labels(topic=DLQ_TOPIC).inc()
        return

    if not seen.add(event_id):
        EVENTS_DUPLICATE.labels(topic=topic).inc()
        return

    EVENTS_CONSUMED.labels(topic=topic).inc()
    fraud = event.get("fraud") or {}
    threat_level = fraud.get("threat_level", "low")
    logger.info("Consumed %s event_id=%s transaction_id=%s threat_level=%s (partition=%s offset=%s)",
                event.get("event_type"), event_id, event.get("transaction_id"), threat_level,
                msg.partition(), msg.offset())

    # A completed transfer that the synchronous check allowed but flagged
    # becomes an alert for downstream review.
    if threat_level != "low":
        alert = {
            "alert_id": f"alert-{event_id}",
            "event_id": event_id,
            "transaction_id": event.get("transaction_id"),
            "sender_id": event.get("sender_id"),
            "receiver_id": event.get("receiver_id"),
            "amount": event.get("amount"),
            "threat_level": threat_level,
            "score": fraud.get("score"),
            "reasons": fraud.get("reasons", []),
            "correlation_id": event.get("correlation_id"),
            "occurred_at": event.get("occurred_at"),
        }
        if _publish(producer, FRAUD_ALERTS_TOPIC, str(event.get("sender_id")), json.dumps(alert).encode()):
            ALERTS_PUBLISHED.labels(threat_level=threat_level).inc()
        else:
            KAFKA_PUBLISH_ERRORS.labels(topic=FRAUD_ALERTS_TOPIC).inc()


def _consume_loop(stop_event: threading.Event) -> None:
    from confluent_kafka import KafkaError

    try:
        consumer = _build_consumer()
        producer = _build_producer()
        consumer.subscribe(KAFKA_TOPICS)
    except Exception:
        logger.exception("Kafka consumer could not start")
        return

    seen = _SeenEvents(DEDUPE_CACHE_SIZE)
    logger.info("Kafka consumer started (group=%s, topics=%s)", KAFKA_CONSUMER_GROUP, KAFKA_TOPICS)
    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Reaching the end of a partition is normal, anything else is logged.
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning("Kafka consumer error: %s", msg.error())
                continue
            try:
                _handle_message(msg, producer, seen)
            except Exception:
                logger.exception("Unexpected error handling message (partition=%s offset=%s)",
                                 msg.partition(), msg.offset())
            try:
                consumer.commit(message=msg, asynchronous=False)
            except Exception:
                logger.exception("Offset commit failed; the message may be delivered again")
    except Exception:
        logger.exception("Kafka consumer loop crashed")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Kafka consumer stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = threading.Event()
    consumer_thread = None
    if KAFKA_ENABLED:
        consumer_thread = threading.Thread(
            target=_consume_loop,
            args=(stop_event,),
            name="kafka-consumer",
            daemon=True,
        )
        consumer_thread.start()
    else:
        logger.info("KAFKA_BOOTSTRAP_SERVERS not set: Kafka consumer disabled")

    yield

    stop_event.set()
    if consumer_thread is not None:
        consumer_thread.join(timeout=10)


app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)
install_request_logging(app, logger)


# =============================================================================
# Synchronous fraud check
# =============================================================================
class ScanRequest(BaseModel):
    amount: Decimal = Field(gt=0)
    sender_id: Optional[int] = None
    receiver_id: Optional[int] = None
    sender_balance: Optional[Decimal] = None
    sender_tx_count_1m: int = Field(default=0, ge=0)
    sender_amount_24h: Decimal = Field(default=Decimal("0"), ge=0)
    sender_account_age_seconds: Optional[int] = Field(default=None, ge=0)
    first_time_receiver: bool = False


def _simulate_work() -> None:
    x = 0.0001
    for _ in range(SIMULATED_WORK_ITERATIONS):
        x += math.sqrt(x)


@app.get("/health/liveness")
def liveness(): return {"status": "alive"}

@app.get("/health/readiness")
def readiness(): return {"status": "ready"}

@app.post("/api/scan")
def scan_transfer(request: ScanRequest):
    """Score one transfer. Stateless: every fact needed arrives in the request."""
    _simulate_work()
    score, threat_level, reasons = score_transfer(TransferFacts(
        amount=request.amount,
        sender_balance=request.sender_balance,
        sender_tx_count_1m=request.sender_tx_count_1m,
        sender_amount_24h=request.sender_amount_24h,
        sender_account_age_seconds=request.sender_account_age_seconds,
        first_time_receiver=request.first_time_receiver,
    ))
    SCANS.labels(threat_level=threat_level).inc()
    for reason in reasons:
        RULE_HITS.labels(rule=reason).inc()
    if threat_level != "low":
        logger.warning("Fraud scan flagged a transfer: threat_level=%s score=%s reasons=%s sender_id=%s",
                       threat_level, score, reasons, request.sender_id)
    return {"status": "scanned", "threat_level": threat_level, "score": score, "reasons": reasons}

@app.get("/api/scan")
def legacy_scan():
    """Old contract (no input, always "low"). Kept so a transaction-service pod
    from the previous image keeps working during a rolling deploy."""
    _simulate_work()
    return {"status": "scanned", "threat_level": "low", "cpu_cycles_used": "high"}
