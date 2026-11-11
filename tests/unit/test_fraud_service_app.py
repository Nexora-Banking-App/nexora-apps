"""In-process tests of fraud-service: API contract, JSON logs, correlation IDs.

    pip install -r fraud-service/requirements.txt pytest httpx
    pytest tests/unit -v
"""
import json
import logging
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "fraud-service"))

fastapi_testclient = pytest.importorskip("fastapi.testclient")

import main  # noqa: E402  (fraud-service/main.py)
import nexora_common  # noqa: E402


@pytest.fixture()
def client():
    with fastapi_testclient.TestClient(main.app) as c:
        yield c


def test_scan_low_risk_transfer(client):
    res = client.post("/api/scan", json={"amount": "25.00", "sender_balance": "1000.00",
                                         "sender_account_age_seconds": 999999})
    assert res.status_code == 200
    body = res.json()
    assert body["threat_level"] == "low" and body["score"] == 0 and body["reasons"] == []


def test_scan_high_risk_transfer(client):
    res = client.post("/api/scan", json={
        "amount": "950.00", "sender_balance": "1000.00", "sender_tx_count_1m": 6,
        "sender_account_age_seconds": 30, "first_time_receiver": True,
    })
    assert res.status_code == 200
    assert res.json()["threat_level"] == "high"


def test_scan_rejects_invalid_amount(client):
    assert client.post("/api/scan", json={"amount": "-5"}).status_code == 422
    assert client.post("/api/scan", json={}).status_code == 422


def test_legacy_get_contract_still_answers(client):
    res = client.get("/api/scan")
    assert res.status_code == 200 and res.json()["threat_level"] == "low"


def test_correlation_id_is_echoed_and_generated(client):
    res = client.get("/api/scan", headers={"X-Correlation-ID": "abc-123"})
    assert res.headers["X-Correlation-ID"] == "abc-123"
    generated = client.get("/api/scan").headers["X-Correlation-ID"]
    assert len(generated) == 36


def _capture_json_logs():
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(json.loads(self.format(record)))

    handler = _Capture()
    handler.setFormatter(nexora_common.JsonFormatter("fraud-service"))
    logging.getLogger().addHandler(handler)
    return records, handler


def test_access_log_is_one_json_line_with_required_fields(client):
    records, handler = _capture_json_logs()
    try:
        client.post("/api/scan", json={"amount": "10"}, headers={"X-Correlation-ID": "corr-1", "X-User-Id": "42"})
        client.get("/health/liveness")
        client.get("/metrics")
    finally:
        logging.getLogger().removeHandler(handler)

    access = [r for r in records if r["message"] == "request"]
    assert len(access) == 1, "health and metrics requests must not be logged"
    line = access[0]
    assert line["service"] == "fraud-service"
    assert line["level"] == "INFO"
    assert line["correlation_id"] == "corr-1"
    assert line["user_id"] == "42"
    assert line["method"] == "POST" and line["path"] == "/api/scan" and line["status"] == 200
    assert isinstance(line["duration_ms"], float)
    assert "timestamp" in line


def test_logs_never_contain_request_headers(client):
    records, handler = _capture_json_logs()
    try:
        client.post("/api/scan", json={"amount": "10"},
                    headers={"Authorization": "Bearer super-secret-token", "X-Internal-Service-Key": "internal-secret"})
    finally:
        logging.getLogger().removeHandler(handler)
    dumped = json.dumps(records)
    assert "super-secret-token" not in dumped and "internal-secret" not in dumped


def test_seen_events_deduplicates_and_is_bounded():
    seen = main._SeenEvents(size=2)
    assert seen.add("a") is True
    assert seen.add("a") is False
    assert seen.add("b") is True
    assert seen.add("c") is True   # evicts "a"
    assert seen.add("a") is True
