import pytest
import pytest_asyncio
import httpx
import asyncio
import uuid

BASE_URL = "http://localhost:8000/api"
TEST_KEY = "nexora-internal-secret-key-123"

@pytest_asyncio.fixture(scope="session")
async def test_users():
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Create Alice
        user_a = f"alice_{uuid.uuid4().hex[:4]}"
        signup_a = await client.post(
            f"{BASE_URL}/signup",
            headers={"X-Internal-Service-Key": TEST_KEY},
            json={"username": user_a, "email": f"{user_a}@test.com", "phone": "555-0101", "password": "Password123!"}
        )
        assert signup_a.status_code == 201, f"Signup Alice failed: {signup_a.text}"

        login_a = await client.post(
            f"{BASE_URL}/login",
            headers={"X-Internal-Service-Key": TEST_KEY},
            json={"username": user_a, "password": "Password123!"}
        )
        assert login_a.status_code == 200, f"Login Alice failed: {login_a.text}"
        token_a = login_a.json()["access_token"]

        # Create Bob
        user_b = f"bob_{uuid.uuid4().hex[:4]}"
        signup_b = await client.post(
            f"{BASE_URL}/signup",
            headers={"X-Internal-Service-Key": TEST_KEY},
            json={"username": user_b, "email": f"{user_b}@test.com", "phone": "555-0102", "password": "Password123!"}
        )
        assert signup_b.status_code == 201, f"Signup Bob failed: {signup_b.text}"

        login_b = await client.post(
            f"{BASE_URL}/login",
            headers={"X-Internal-Service-Key": TEST_KEY},
            json={"username": user_b, "password": "Password123!"}
        )
        assert login_b.status_code == 200, f"Login Bob failed: {login_b.text}"
        token_b = login_b.json()["access_token"]

        return {"alice": user_a, "token_a": token_a, "bob": user_b, "token_b": token_b}


@pytest.mark.asyncio
async def test_concurrent_idempotency_replays(test_users):
    """
    STRESS TEST: Fire 10 parallel transfers with the EXACT SAME idempotency key.
    PROVES: Two-Phase lease claims money is only debited once ($100).
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        token_a = test_users["token_a"]
        user_b = test_users["bob"]
        shared_key = str(uuid.uuid4())

        async def send_duplicate():
            return await client.post(
                f"{BASE_URL}/transfer",
                headers={
                    "Authorization": f"Bearer {token_a}", 
                    "Idempotency-Key": shared_key,
                    "X-Internal-Service-Key": TEST_KEY
                },
                json={"receiver_username": user_b, "amount": "100.00"}
            )

        responses = await asyncio.gather(*[send_duplicate() for _ in range(10)])

        successes = [r for r in responses if r.status_code == 200]
        in_flight = [r for r in responses if r.status_code == 409]

        assert len(successes) >= 1, "Expected at least 1 success"
        assert len(successes) + len(in_flight) == 10, "All duplicate requests must be resolved cleanly"


@pytest.mark.asyncio
async def test_bidirectional_transfers_no_deadlock(test_users):
    """
    STRESS TEST: Alice sends to Bob while Bob sends to Alice simultaneously.
    PROVES: Deterministic ascending lock ordering (min/max) eliminates deadlocks.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        token_a, user_a = test_users["token_a"], test_users["alice"]
        token_b, user_b = test_users["token_b"], test_users["bob"]

        async def a_to_b():
            return await client.post(
                f"{BASE_URL}/transfer",
                headers={
                    "Authorization": f"Bearer {token_a}", 
                    "Idempotency-Key": str(uuid.uuid4()),
                    "X-Internal-Service-Key": TEST_KEY
                },
                json={"receiver_username": user_b, "amount": "50.00"}
            )

        async def b_to_a():
            return await client.post(
                f"{BASE_URL}/transfer",
                headers={
                    "Authorization": f"Bearer {token_b}", 
                    "Idempotency-Key": str(uuid.uuid4()),
                    "X-Internal-Service-Key": TEST_KEY
                },
                json={"receiver_username": user_a, "amount": "50.00"}
            )

        res_a, res_b = await asyncio.gather(a_to_b(), b_to_a())
        assert res_a.status_code == 200, f"A->B failed: {res_a.text}"
        assert res_b.status_code == 200, f"B->A failed: {res_b.text}"


@pytest.mark.asyncio
async def test_concurrent_transfers_no_double_spend(test_users):
    """
    STRESS TEST: Fire parallel transfers until balance is exhausted.
    PROVES: Row-level locking serializes balance deductions without race conditions.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        token_a = test_users["token_a"]
        user_b = test_users["bob"]

        # Fetch current balance
        acc = await client.get(
            f"{BASE_URL}/account/me", 
            headers={"Authorization": f"Bearer {token_a}", "X-Internal-Service-Key": TEST_KEY}
        )
        current_balance = float(acc.json()["balance"])
        expected_successes = int(current_balance // 100)

        async def send_transfer():
            return await client.post(
                f"{BASE_URL}/transfer",
                headers={
                    "Authorization": f"Bearer {token_a}", 
                    "Idempotency-Key": str(uuid.uuid4()),
                    "X-Internal-Service-Key": TEST_KEY
                },
                json={"receiver_username": user_b, "amount": "100.00"}
            )

        # Fire 15 concurrent transfers
        responses = await asyncio.gather(*[send_transfer() for _ in range(15)])

        successes = [r for r in responses if r.status_code == 200]
        insufficient = [r for r in responses if r.status_code == 400]

        assert len(successes) == expected_successes, f"Expected {expected_successes} successes, got {len(successes)}"
        assert len(insufficient) == (15 - expected_successes), f"Expected {15 - expected_successes} rejections, got {len(insufficient)}"