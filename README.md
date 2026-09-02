# Nexora Core Banking Platform: Workload Architecture & Engineering Reference

An institutional-grade, distributed core banking application workload designed for the **Nexora Enterprise GitOps Platform**. This project demonstrates a Zero-Trust microservices architecture with edge token verification, a Two-Phase Lease idempotency engine with monotonic **Fencing Tokens**, double-entry bookkeeping, scoped internal RPC authorization, automated concurrency test suites, and ACID-compliant distributed locking.

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Architecture](#architecture)
4. [Prerequisites](#prerequisites)
5. [Local Development with Docker Compose](#local-development-with-docker-compose)
6. [Microservice Domain & Service Specifications](#microservice-domain--service-specifications)
7. [Financial Concurrency & Fencing-Token Engine](#financial-concurrency--fencing-token-engine)
8. [Failure Decision & Idempotency State Matrix](#failure-decision--idempotency-state-matrix)
9. [Security Hardening: Least Privilege & Information Disclosure](#security-hardening-least-privilege--information-disclosure)
10. [Secrets & Configuration Management](#secrets--configuration-management)
11. [Container Optimization & Multi-Stage Builds](#container-optimization--multi-stage-builds)
12. [Metrics & Observability Instrumentation](#metrics--observability-instrumentation)
13. [Automated Concurrency Testing & System Verification](#automated-concurrency-testing--system-verification)
14. [Real-World Troubleshooting & Solutions](#real-world-troubleshooting--solutions)
15. [Known Limitations & Phase 1 Infrastructure Scope](#known-limitations--phase-1-infrastructure-scope)
16. [Screenshots Index](#screenshots-index)

---

## Overview

The Nexora banking workload is structured as a polyglot, domain-driven microservice system designed to rigorously validate cloud-native platform infrastructure (Kubernetes Ingress, Cilium Network Policies, Istio mTLS, External Secrets Operator, AWS RDS, and Prometheus/HPA autoscaling).

| Service | Technology | Responsibility | Port |
|---|---|---|---|
| `frontend-web` | NGINX 1.25 / Alpine / Vanilla JS / Tailwind | Client Single-Page App (SPA), Tab-isolated Session Manager, Version-Banner Suppressed | 8080 (Mapped: 80) |
| `api-gateway` | Python 3.11 / FastAPI / Asynchronous `httpx` | Edge Rate-Limiting, Distributed Tracing (`X-Correlation-ID`), JWT Verification, Server Header Stripping | 8000 |
| `auth-service` | Python 3.11 / FastAPI / `bcrypt` / `PyJWT` | Identity Management, Password Hashing, Scoped Token Minting, Deterministic Grant Delegation | 8000 (Internal) |
| `account-service` | Python 3.11 / FastAPI / `DBUtils` / `PyMySQL` | Strictly Read-Only CQRS Ledger, Paginated Account Queries | 8000 (Internal) |
| `transaction-service` | Python 3.11 / FastAPI / `Decimal` / `httpx` | Sole Mutation Authority, Deterministic Locks, Fencing-Token Lease Engine, Treasury-Scoped Internal RPC | 8000 (Internal) |
| `fraud-service` | Python 3.11 / FastAPI / `prometheus-fastapi` | CPU-Intensive Risk Engine, HPA & Prometheus Golden Signals Target | 8000 (Internal) |
| `mysql-db` / AWS RDS | MySQL 8.0 (InnoDB) | Relational ACID Persistence, Row-Level Locking, Composite Unique Constraints | 3306 |

---

## Quick Start

```bash
git clone https://github.com/nexora-platform/nexora-apps.git
cd nexora-apps
```

**1. Spin up the complete stack locally**
```bash
docker compose down -v
docker compose build --no-cache
docker compose up
```

**2. Access the Application Dashboard**
Open your browser to:
```text
http://localhost:8080
```

**3. Test Peer-to-Peer Transfers**
* Open **Tab 1**: Register as `ahmed` (password: `password123`). Log in -> Balance will display **$1,000.00** from the Treasury Reserve.
* Open **Tab 2**: Register as `omar` (password: `password123`). Log in -> Balance will display **$1,000.00**.
* In **Tab 1**, transfer `$250.00` to `omar`.
* In **Tab 2**, observe omar's balance update to **$1,250.00** and Ahmed's balance update to **$750.00**.

```text
Note: Giving each user $1000 on signup is designed for testing purposes and not a real scenario.
```

---

## Architecture

### System Topology

```text
                                  [ CLIENT BROWSER / SPA ]
                                             │
                     ┌───────────────────────┴───────────────────────┐
                     │ (Port 8080 / Static Assets)                   │ (Port 8000 / Dynamic API)
                     ▼                                               ▼
         [ frontend-web (Nginx) ]                        [ api-gateway (FastAPI) ]
         • SPA Dashboard (Tailwind CSS)                  • Perimeter Rate Limiting (Sliding Window)
         • Client-Side UUID Idempotency-Keys             • Distributed Tracing (X-Correlation-ID)
         • Tab-Isolated Session Storage                  • Edge JWT Verification & Claim Stripping
         • server_tokens off (No Version Banner)         • Spec-Valid CORS (Explicit Origins)
                                                         • Transparent CORS OPTIONS Bypass
                                                         • Trusted Downstream Header Injection (X-User-Id)
                                                         • Outbound Server Header Stripping
                                                                     │
                       ┌─────────────────────────────────────────────┼─────────────────────────────────────────────┐
                       │                                             │                                             │
                       ▼ (Public Routing)                            ▼ (Protected Routing)                         ▼ (Protected Routing)
             [ auth-service ]                              [ account-service ]                           [ transaction-service ]
             • Salting & Bcrypt Hashing                    • Strictly Read-Only CQRS Ledger              • 1. Phase 1: Fast Lease (<1ms)
             • 15-min JWT Minting (HS256)                  • Paginated Queries (Limit/Offset)            • 2. Phase 2: Unconnected Fraud RPC
             • Role & Scope-Enriched Claims                • Zero Side Effects on GET /account/me        • 3. Phase 3: Fast Mutation (<5ms)
             • Deterministic Grant RPC Delegation          • Lazy Connection Pooling                     • 4. Monotonic Fencing Tokens
             • Lazy Connection Pooling                     • Lifespan Graceful Draining                  • 5. Sole Ledger Mutation Authority
             • Lifespan Graceful Draining                                                                • 6. Treasury-Scoped Internal RPC
                       │                                             │                                             │
                       │ (Pooled DB Connections)                     │ (Pooled DB Connections)                     │ (Pooled DB Connections)
                       └─────────────────────────────────────────────┼─────────────────────────────────────────────┘
                                                                     │                                             │ (Synchronous HTTP / 2s Timeout)
                                                                     ▼                                             ▼
                                                        [ MySQL 8.0 / AWS RDS ]                           [ fraud-service ]
                                                        • InnoDB Engine (ACID)                            • CPU-Intensive Math Loops
                                                        • Row-Level Locking (FOR UPDATE)                  • Prometheus Metrics Target
                                                        • Composite Constraints (user_id, key)            • HPA Autoscaling Target
```

---

## Prerequisites

* Docker Engine (>= 24.0) & Docker Compose V2
* Python 3.11+ (for local test runner execution)
* `pytest`, `pytest-asyncio`, and `httpx` (for running the automated concurrency test suite)
* Modern Web Browser (Chrome, Firefox, Brave, Edge)

---

## Local Development with Docker Compose

Local development runs the entire 7-tier microservice architecture connected over an internal Docker bridge network (`nexora_default`). Internal backend services (`auth-service`, `account-service`, `transaction-service`, `fraud-service`) use `expose` rather than `ports`, preventing direct host access and mimicking a private Kubernetes subnet.

```bash
docker compose up --build
```
![alt text](screenshots/docker-compose-up.png)

### Endpoints
* **Frontend Portal:** `http://localhost:8080`
* **API Gateway Health:** `http://localhost:8000/health/liveness`
* **Prometheus Metrics (Gateway):** `http://localhost:8000/metrics`
* **MySQL Database:** `localhost:3306` (`user: dbuser`, `password: dbpassword`, `database: nexora_bank`)

---

## Microservice Domain & Service Specifications

### 1. `frontend-web`
* **Session Management:** Stores short-lived access tokens in `sessionStorage` rather than `localStorage`, enabling isolated multi-tab concurrent user simulation.
* **Idempotency Generation:** Automatically generates a client-side UUID (`crypto.randomUUID()`) attached as an `Idempotency-Key` header on every financial mutation.
* **Information Disclosure Hardening:** `server_tokens off` in the Nginx configuration suppresses the version banner from all HTTP responses.

### 2. `api-gateway`
* **Perimeter Rate Limiting:** Enforces in-memory sliding-window throttles (`/login`: max 5 req/min per IP; `/signup`: max 3 req/hr per IP).
* **Distributed Tracing:** Inspects incoming traffic for `X-Correlation-ID`. If missing, mints a UUID and propagates it across downstream HTTP headers and client responses.
* **Edge Authentication:** Verifies JWT signatures (`HS256`, 15-minute expiration), drops untrusted client headers, and injects verified `X-User-Id` and `X-Username` headers downstream.
* **CORS Specification:** Strictly enforces explicit allowed origins (`CORS_ORIGINS`) with `allow_credentials=True` and provides an unauthenticated bypass for HTTP `OPTIONS` preflight checks.
* **Information Disclosure Hardening:** Strips the outbound `server` header from every proxied response, preventing Uvicorn version fingerprinting.

### 3. `auth-service`
* **Password Hashing:** 12-round salted hashing using the official C-optimized `bcrypt` library.
* **Atomic Identity Provisioning:** Creates user credentials and opens an account with `$0.00` in a single SQL transaction.
* **Scoped Token Minting:** Issues JWTs enriched with `role: customer` and `scope: [account:read, transfer:create]` claims, establishing a forward-compatible least-privilege model without requiring a schema migration to add new roles later.
* **Deterministic Grant Delegation:** Delegates the $1,000.00 welcome grant to `transaction-service` using a deterministic key (`grant-user-{id}`). In case of network timeouts, local state is preserved without destructive rollbacks, making the grant safely retryable.

### 4. `account-service`
* **Strictly Read-Only (Zero Side Effects):** Operates purely as a CQRS read model. Never creates money or alters balances on `GET /account/me`. Raises `404 Not Found` if an account is unprovisioned.
* **Paginated Queries:** Implements `limit` (max 50) and `offset` SQL parameters for transaction history.

### 5. `transaction-service`
* **Sole Mutation Authority:** The single service in the entire platform permitted to alter account balances. Exposes `/transfer` (customer peer-to-peer) and `/internal/system-transfer` (constant-time verified system grants via `secrets.compare_digest`).
* **Treasury-Scoped Internal RPC (Least Privilege):** `/internal/system-transfer` rejects any request where `sender_id != 1` with `HTTP 403 Forbidden`. The internal service secret authenticates the *caller*, but this guard additionally restricts *what* the caller is authorized to do — a leaked key can only ever debit the Treasury Reserve, never move funds between arbitrary user accounts.
* **Deterministic Lock Ordering:** Sorts sender and receiver IDs (`min(sender, receiver)` -> `max(sender, receiver)`) before acquiring `SELECT ... FOR UPDATE` row locks, mathematically preventing deadlocks.
* **Exact Decimal Math:** Python `Decimal` + SQL `DECIMAL(15,2)` strictly enforced.

### 6. `fraud-service`
* **Risk Engine Simulation:** Heavy mathematical loops simulating CPU-intensive algorithmic scoring.
* **Observability:** Exposes latency, saturation, error rate, and request volume via `prometheus-fastapi-instrumentator` on `/metrics`.

![alt text](screenshots/signup-screen.png)
![alt text](screenshots/dashboard-ahmed.png)
![alt text](screenshots/dashboard-omar.png)

---

## Financial Concurrency & Fencing-Token Engine

Holding open database connections across external network I/O boundaries causes connection pool exhaustion under load. Nexora solves this via a **Two-Phase Lease Pattern with Monotonic Fencing Tokens (Kleppmann Pattern)**.

```text
[ Phase 1: Atomic Lease Claim & Fencing Token Minting (<1ms) ]
  ├── Check out DB connection
  ├── INSERT INTO idempotency_records (user_id, idempotency_key, 'PROCESSING', lease_version = 1)
  ├── COMMIT & RELEASE connection back to pool immediately!
  └── (If duplicate & status == 'PROCESSING' for >10s, execute Atomic CAS Reclaim with lease_version = lease_version + 1!)

[ Phase 2: External Risk Gating (Zero DB Connections Held) ]
  ├── Zero DB connections pinned during the 2.0s window
  └── Execute synchronous HTTP call to fraud-service (2.0s timeout)

[ Phase 3: Financial Execution & Fencing Commit Gate (<5ms) ]
  ├── Check out fresh DB connection
  ├── BEGIN TRANSACTION
  ├── Ascending Row Locks: SELECT ... FOR UPDATE (min_id, max_id)
  ├── Balance checks & Decimal mutations
  ├── INSERT INTO transactions audit record
  ├── UPDATE idempotency_records SET status = 'COMPLETED' WHERE lease_version = acquired_version
  │     ├── rowcount == 1 -> COMMIT & RELEASE connection
  │     └── rowcount == 0 -> ROLLBACK! (Zombie worker detected: lease was reclaimed during Phase 2)
```

### The Zombie Worker Protection (Fencing Token Query)
If a worker suffers an asynchronous pause (e.g., GC stall or network delay) during Phase 2, a concurrent retry reclaims the lease by incrementing `lease_version`. When the stalled worker wakes up, its Phase 3 commit is rejected at the database engine level:

```sql
UPDATE idempotency_records 
SET status = 'COMPLETED', response_body = ?
WHERE user_id = ? AND idempotency_key = ? AND lease_version = ?;
```
* `rowcount == 1`: The worker still holds the active lease; transaction commits safely.
* `rowcount == 0`: Fencing token mismatch. The worker rolls back immediately without mutating balances, eliminating split-brain double-spending.

---

## Failure Decision & Idempotency State Matrix

| Event / Outcome | Classification | Phase | DB Connection Held During Error? | Idempotency Record State | Client Response on Retry |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Transfer Success** | Successful Execution | Phase 3 | Yes (<5ms) | `COMPLETED` (success JSON) | Replays cached success message |
| **Insufficient Funds** | Terminal Business Decline | Phase 3 | Yes (<5ms) | `COMPLETED` (decline JSON) | Replays cached "Insufficient funds" |
| **Recipient Not Found** | Terminal Business Decline | Phase 3 | Yes (<5ms) | `COMPLETED` (decline JSON) | Replays cached "User not found" |
| **Fraud Flagged High** | Terminal Risk Decline | Phase 2 | Yes (<1ms write) | `COMPLETED` (decline JSON) | Replays cached "Fraud declined" |
| **Fraud Engine Timeout (2s)** | Transient System Failure | Phase 2 | **NO (0 connections)** | Row deleted (Unconsumed) | Re-executes as a new attempt |
| **Worker Pod Crashes Mid-Flight** | Unhandled Process Death | Phase 2 | **NO (0 connections)** | `PROCESSING` (Stale >10s) | Next retry reclaims lease via CAS + increments `lease_version` |
| **Zombie Worker Awakens** | Split-Brain Race Condition | Phase 3 | Yes (<1ms check) | Unchanged (`COMPLETED`) | Rollback triggered; returns `HTTP 409 Conflict` |
| **System Transfer with sender_id != 1** | Privilege Escalation Attempt | Pre-Phase 1 | No connection checked out | No record created | Returns `HTTP 403 Forbidden` immediately |

---

## Security Hardening: Least Privilege & Information Disclosure

Two application-layer hardening passes were applied on top of the core concurrency and identity model, closing gaps standard in a fintech security review.

### 1. Information Disclosure Prevention (Banner Grabbing)
By default, both Nginx and Uvicorn advertise their exact software version in the `Server` response header, giving an attacker a direct lookup table of known CVEs to try first.
* **`frontend-web`:** `server_tokens off;` set in the Nginx server block, removing the version string from every response.
* **`api-gateway`:** The response-handling middleware explicitly deletes the `server` header from every outgoing response before it reaches the client, regardless of what Uvicorn attaches by default.

```python
response = await call_next(request)
response.headers["X-Correlation-ID"] = correlation_id
if "server" in response.headers:
    del response.headers["server"]
return response
```

### 2. Least-Privilege Scoping (Internal RPC & Token Claims)
Authentication proves *who* is calling; authorization should still constrain *what* they're allowed to do once verified. Two guards were added on that basis:

* **Treasury-Scoped Internal Transfers:** `/internal/system-transfer` in `transaction-service` now rejects any request where `sender_id != 1` with `403 Forbidden`, even if the caller presents a valid `X-Internal-Service-Key`. Previously, possession of that one shared secret was sufficient to move funds between *any* two accounts on the platform; it is now hard-restricted to Treasury Reserve debits only, matching its actual intended purpose (onboarding grants).
* **Scoped JWT Claims:** Access tokens minted by `auth-service` now carry `role: "customer"` and `scope: ["account:read", "transfer:create"]` claims. No endpoint currently enforces these claims (there is only one role today), but the claim shape is in place so a future role (e.g., `support`, `admin`) or a narrower scope check can be added at the gateway or service level without a breaking change to the token format.

```python
if payload.sender_id != 1:
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Forbidden: System transfer key is restricted strictly to Treasury Reserve debits (sender_id=1)"
    )
```

**Verification:**
```bash
# Server header stripping
curl -I http://localhost:8000/health/liveness
# (No "server: uvicorn" header present)

# Treasury-scoping enforcement
curl -i -X POST "http://localhost:8000/internal/system-transfer" \
  -H "X-Internal-Service-Key: nexora-internal-secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{"sender_id": 2, "receiver_id": 3, "amount": "50.00", "idempotency_key": "hack-1"}'
# -> 403 Forbidden: System transfer key is restricted strictly to Treasury Reserve debits
```

---

## Secrets & Configuration Management

Configuration and secrets are strictly decoupled from application code following 12-Factor principles:

* **Environment Variables:**
  * `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`: Database target connectivity.
  * `JWT_SECRET`: Secret key for HS256 token minting and perimeter validation.
  * `INTERNAL_SERVICE_SECRET`: Constant-time verified key (`secrets.compare_digest`) for internal RPC calls, additionally scoped server-side to Treasury-only operations.
  * `CORS_ORIGINS`: Comma-separated allowed origins (e.g. `http://localhost:8080`).
  * `FRAUD_SVC_URL`, `ACCOUNT_SVC_URL`, `TRANSACTION_SVC_URL`, `AUTH_SVC_URL`: Service discovery endpoints.
* **Kubernetes Integration (Upcoming Phase 3):**
  * `app-secrets` will be dynamically synced from **AWS Secrets Manager** via the **External Secrets Operator (ESO)** and injected at pod runtime.

---

## Container Optimization & Multi-Stage Builds

All Python microservices utilize hardened, multi-stage Docker builds based on Debian `slim`:

```dockerfile
# STAGE 1: Builder (Compiles dependencies in isolated virtualenv)
FROM python:3.11-slim AS builder
WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# STAGE 2: Runner (Minimal attack surface, unprivileged user)
FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY main.py .
RUN addgroup --system appgroup && adduser --system --group appuser
USER appuser
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`frontend-web` follows the same hardening discipline at the web-server layer:

```dockerfile
FROM nginx:alpine
# Security: Disable Nginx version banner (server_tokens off in default.conf)
COPY default.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

---

## Metrics & Observability Instrumentation

### 1. Prometheus Golden Signals
Every microservice instruments FastAPI via `prometheus-fastapi-instrumentator` exposing standard endpoints at `/metrics`:
* **Latency:** Request duration histograms.
* **Traffic:** Rate of HTTP requests per second partitioned by method, endpoint, and status code.
* **Errors:** HTTP 4xx and 5xx error rates.
* **Saturation:** Thread-pool utilization and connection queue depth.

### 2. Distributed Tracing
The API Gateway generates or propagates an `X-Correlation-ID` header across all downstream RPCs, tying client requests, microservice logs, and database errors into a single unified trace.

![alt text](screenshots/prometheus-metrics.png)

---

## Automated Concurrency Testing & System Verification

The repository includes an automated integration test suite in `tests/test_concurrency.py` that executes parallel asynchronous requests against the running cluster to empirically validate concurrency guarantees.

### 1. Running the Automated Concurrency Tests
```bash
pip install pytest pytest-asyncio httpx
pytest tests/test_concurrency.py -v
```

### 2. Test Suite Specifications

```text
tests/test_concurrency.py::test_concurrent_transfers_no_double_spend PASSED   [ 33%]
tests/test_concurrency.py::test_concurrent_idempotency_replays PASSED         [ 66%]
tests/test_concurrency.py::test_bidirectional_transfers_no_deadlock PASSED   [100%]

============================== 3 passed in 1.42s ==============================
```

* **Test 1 (`test_concurrent_transfers_no_double_spend`):** Fires 10 simultaneous $200 transfers from an account with $1,000. Asserts that row-level locking strictly serializes execution to produce exactly 5 successes ($1,000 debited) and 5 rejections for insufficient funds, with ending balance $0.00.
* **Test 2 (`test_concurrent_idempotency_replays`):** Fires 10 simultaneous transfers sharing the exact same `Idempotency-Key` UUID. Asserts that the atomic lease ensures money is debited exactly once ($200) and all requests return 200 or 409.
* **Test 3 (`test_bidirectional_transfers_no_deadlock`):** Fires simultaneous cross-transfers (User A -> User B and User B -> User A). Asserts that deterministic ascending lock ordering (`min/max`) eliminates SQL deadlocks.

![](screenshots/test.png)

*(Note: The fencing token commit gate and the treasury-scoping guard are verified by database constraint enforcement, direct `curl` testing, and code analysis, as black-box HTTP tests cannot easily simulate an asynchronous OS thread pause mid-RPC without synthetic database latency injection).*

---

## Real-World Troubleshooting & Solutions

This section documents the actual technical bugs encountered during the workload engineering process, root-cause diagnoses, and permanent architectural fixes applied.

### 1. MySQL 8.0 `caching_sha2_password` Authentication Crash
* **Symptom:** `pymysql.err.OperationalError: RuntimeError: 'cryptography' package is required for sha256_password or caching_sha2_password auth methods`.
* **Diagnosis:** MySQL 8.0 defaults to SHA-2 authentication. `PyMySQL` requires the C-based `cryptography` Python package to perform public key encryption during the authentication handshake.
* **Fix:** Added `cryptography==41.0.3` to all service dependencies rather than downgrading database engine security.

### 2. Pydantic `EmailStr` Startup Exception in `auth-service`
* **Symptom:** `auth-service` crashed on startup with `ImportError: email-validator is not installed`.
* **Diagnosis:** Pydantic's `EmailStr` field type has an implicit dependency on `email-validator`. Without it, Uvicorn crashed before initializing routes.
* **Fix:** Pinned `email-validator>=2.0.0` in `requirements.txt` and stabilized the data model.

### 3. Eager Connection Pool Initialization Crash on Database Boot
* **Symptom:** `account-service` and `transaction-service` crashed immediately upon `docker compose up` with `pymysql.err.OperationalError: (2003, "Can't connect to MySQL server on 'mysql-db' [Errno 111] Connection refused")`.
* **Diagnosis:** `PooledDB` was configured with `mincached=5`, forcing immediate TCP socket creation on module import while MySQL was still initializing its InnoDB storage engine.
* **Fix:** Configured `mincached=0` (Lazy Initialization). Connection sockets are deferred until the first HTTP request or readiness probe arrives.

### 4. Spec-Invalid Wildcard CORS with Credentials
* **Symptom:** Browser rejected API Gateway responses with credentials mode enabled when configured with `allow_origins=["*"]`.
* **Diagnosis:** The W3C CORS specification forbids wildcard origins when `allow_credentials` is true.
* **Fix:** Replaced wildcard origin with explicit environment-bound origins (`CORS_ORIGINS` defaulting to `http://localhost:8080`).

### 5. CORS Preflight `OPTIONS` 401 Interception at Gateway
* **Symptom:** Browser failed to load dashboard balances; terminal logs revealed `OPTIONS /api/account/me HTTP/1.1 401 Unauthorized`.
* **Diagnosis:** The API Gateway JWT authentication middleware intercepted browser preflight `OPTIONS` requests before the CORS middleware could negotiate access headers.
* **Fix:** Added an explicit preflight bypass: `if request.method == "OPTIONS": return await call_next(request)` at the top of the Gateway middleware stack.

### 6. Incognito Cross-Tab `localStorage` Contamination
* **Symptom:** Opening a second incognito tab automatically logged into the user account of the first tab, preventing multi-user transfer testing.
* **Diagnosis:** Web browsers share `localStorage` across all incognito tabs in the same session window.
* **Fix:** Switched client token storage to **`sessionStorage`**, guaranteeing complete per-tab session isolation.

### 7. Ambiguous Network Timeout vs. Destructive Compensation Hazard
* **Symptom:** Onboarding grants that timed out over the network could trigger local user deletion, causing foreign-key crashes and unallocated treasury debits.
* **Diagnosis:** Network timeouts are ambiguous states; deleting state on timeout violates distributed computing safety.
* **Fix:** Switched onboarding keys to deterministic values (`grant-user-{id}`) without random UUIDs and eliminated local deletes, making grant RPCs safely retryable.

### 8. Unscoped Internal Service Key (Privilege Escalation Risk)
* **Symptom:** `INTERNAL_SERVICE_SECRET`, while constant-time verified, granted the ability to debit *any* account, not just the Treasury Reserve — the key authenticated the caller but placed no limit on the action.
* **Diagnosis:** Authentication (proving who is calling) had not been paired with authorization (limiting what the caller may do) on the internal RPC path.
* **Fix:** Added an explicit `sender_id != 1` guard rejecting any system transfer not originating from the Treasury Reserve account, regardless of key validity.

---

## Known Limitations & Phase 1 Infrastructure Scope

The application layer is intentionally scoped to what belongs in code. The following are known, named gaps — not oversights — deferred to infrastructure phases:

* **No TLS/HTTPS:** All traffic (browser-to-gateway and service-to-service) currently runs over plain HTTP. TLS termination requires a Load Balancer/Ingress with an attached certificate — infrastructure that doesn't exist in local Docker Compose. Planned for Phase 1 (AWS ALB / Kubernetes Ingress).
* **No WAF:** No layer currently inspects payloads for generic attack signatures (SQLi patterns, known exploit shapes) ahead of the application-aware API gateway. Planned via AWS WAF attached to the Ingress in Phase 1.
* **Rate limiting is single-replica only:** The gateway's sliding-window limiter is in-memory; under multiple `api-gateway` replicas, the effective limit multiplies per replica. A production deployment would move this to a shared store (Redis) or offload it to an infra-layer rate-limiting feature.
* **No refresh tokens:** JWTs expire after 15 minutes with no silent renewal; the user must log in again. This is a deliberate trade-off (short blast radius vs. added complexity), not an oversight — revisiting it is tied to the HTTPS/cookie-security work in Phase 1.
* **No internal mTLS / network policy enforcement:** Internal services currently trust the `X-User-Id` header on the assumption that only `api-gateway` can reach them. That assumption is not yet enforced at the network layer — any container on the same Docker network can currently reach internal services directly. Cilium NetworkPolicies (Phase 1) are required to make this a real guarantee rather than an implicit one.
* **No automatic reconciliation for failed onboarding grants:** If the treasury grant RPC fails during signup, the account is left at $0.00 in a safely retryable state (deterministic idempotency key), but nothing currently triggers that retry automatically. A scheduled reconciliation job is a reasonable Phase 1+ addition.

---

## Screenshots Index

Quick reference for architectural verification screenshots.

| File | Shows | Section |
|---|---|---|
| `screenshots/signup-screen.png` | Nexora Bank Authentication Interface | Local Development |
| `screenshots/dashboard-ahmed.png` | Ahmed Dashboard ($1,000.00 Balance & Treasury Grant) | System Verification |
| `screenshots/dashboard-omar.png` | omar Dashboard ($1,250.00 Balance & Incoming Credit) | System Verification |
| `screenshots/docker-compose-up.png` | All 7 Microservice Containers Healthy | Local Development |
| `screenshots/prometheus-metrics.png` | Golden Signals Scraped on `/metrics` | Observability |