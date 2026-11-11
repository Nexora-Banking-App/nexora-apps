"""Stateless fraud rules.

fraud-service keeps no state and has no database. transaction-service reads the
facts below from MySQL and sends them with every scan request; this module turns
them into a score. Because nothing is stored here, any number of HPA replicas
give the same answer.

Score -> threat level:
    score >= HIGH_THRESHOLD    -> "high"    (transfer is declined)
    score >= MEDIUM_THRESHOLD  -> "medium"  (transfer is allowed and flagged)
    otherwise                  -> "low"

Every threshold can be changed with an environment variable of the same name
prefixed with FRAUD_ (for example FRAUD_LARGE_AMOUNT=20000).
"""
import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional, Tuple


def _env_decimal(name: str, default: str) -> Decimal:
    return Decimal(os.getenv(f"FRAUD_{name}", default))


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(f"FRAUD_{name}", str(default)))


@dataclass(frozen=True)
class RuleConfig:
    # Single-transfer size
    large_amount: Decimal = field(default_factory=lambda: _env_decimal("LARGE_AMOUNT", "10000"))
    elevated_amount: Decimal = field(default_factory=lambda: _env_decimal("ELEVATED_AMOUNT", "5000"))
    # Velocity: completed transfers by the sender in the last 60 seconds
    velocity_high: int = field(default_factory=lambda: _env_int("VELOCITY_HIGH", 10))
    velocity_elevated: int = field(default_factory=lambda: _env_int("VELOCITY_ELEVATED", 5))
    # Total sent in the last 24 hours, including this transfer
    daily_limit: Decimal = field(default_factory=lambda: _env_decimal("DAILY_LIMIT", "20000"))
    # Draining most of the balance in one transfer
    drain_ratio: Decimal = field(default_factory=lambda: _env_decimal("DRAIN_RATIO", "0.9"))
    drain_min_amount: Decimal = field(default_factory=lambda: _env_decimal("DRAIN_MIN_AMOUNT", "500"))
    # Brand-new account moving real money
    new_account_seconds: int = field(default_factory=lambda: _env_int("NEW_ACCOUNT_SECONDS", 3600))
    new_account_min_amount: Decimal = field(default_factory=lambda: _env_decimal("NEW_ACCOUNT_MIN_AMOUNT", "500"))
    # First payment to this receiver
    first_receiver_min_amount: Decimal = field(default_factory=lambda: _env_decimal("FIRST_RECEIVER_MIN_AMOUNT", "1000"))
    # Decision thresholds
    medium_threshold: int = field(default_factory=lambda: _env_int("MEDIUM_THRESHOLD", 40))
    high_threshold: int = field(default_factory=lambda: _env_int("HIGH_THRESHOLD", 70))


@dataclass(frozen=True)
class TransferFacts:
    amount: Decimal
    sender_balance: Optional[Decimal] = None
    sender_tx_count_1m: int = 0
    sender_amount_24h: Decimal = Decimal("0")
    sender_account_age_seconds: Optional[int] = None
    first_time_receiver: bool = False


def score_transfer(facts: TransferFacts, config: Optional[RuleConfig] = None) -> Tuple[int, str, List[str]]:
    """Return (score 0-100, threat level, reasons)."""
    cfg = config or RuleConfig()
    score = 0
    reasons: List[str] = []

    def hit(points: int, reason: str) -> None:
        nonlocal score
        score += points
        reasons.append(reason)

    # 1. Size of this transfer
    if facts.amount >= cfg.large_amount:
        hit(40, "large_amount")
    elif facts.amount >= cfg.elevated_amount:
        hit(20, "elevated_amount")

    # 2. Velocity
    if facts.sender_tx_count_1m >= cfg.velocity_high:
        hit(60, "velocity_high")
    elif facts.sender_tx_count_1m >= cfg.velocity_elevated:
        hit(30, "velocity_elevated")

    # 3. 24-hour volume
    if facts.sender_amount_24h + facts.amount >= cfg.daily_limit:
        hit(30, "daily_volume")

    # 4. Balance drain
    if (
        facts.sender_balance is not None
        and facts.sender_balance > 0
        and facts.amount >= cfg.drain_min_amount
        and facts.amount / facts.sender_balance >= cfg.drain_ratio
    ):
        hit(25, "balance_drain")

    # 5. New account
    if (
        facts.sender_account_age_seconds is not None
        and facts.sender_account_age_seconds < cfg.new_account_seconds
        and facts.amount >= cfg.new_account_min_amount
    ):
        hit(30, "new_account")

    # 6. First payment to this receiver
    if facts.first_time_receiver and facts.amount >= cfg.first_receiver_min_amount:
        hit(15, "first_time_receiver")

    score = min(score, 100)
    if score >= cfg.high_threshold:
        level = "high"
    elif score >= cfg.medium_threshold:
        level = "medium"
    else:
        level = "low"
    return score, level, reasons
