"""Unit tests for the stateless fraud rules. No services or database needed:

    pip install pytest
    pytest tests/unit -v
"""
import pathlib
import sys
from decimal import Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "fraud-service"))

from rules import RuleConfig, TransferFacts, score_transfer  # noqa: E402

CFG = RuleConfig()


def facts(**overrides) -> TransferFacts:
    base = dict(
        amount=Decimal("50.00"),
        sender_balance=Decimal("1000.00"),
        sender_tx_count_1m=0,
        sender_amount_24h=Decimal("0"),
        sender_account_age_seconds=30 * 24 * 3600,
        first_time_receiver=False,
    )
    base.update(overrides)
    return TransferFacts(**base)


def test_small_transfer_from_established_account_is_low():
    score, level, reasons = score_transfer(facts(), CFG)
    assert (score, level, reasons) == (0, "low", [])


def test_small_transfers_from_a_new_account_stay_low():
    # A fresh signup sending its welcome grant in $100 pieces must not be flagged.
    score, level, _ = score_transfer(facts(amount=Decimal("100"), sender_account_age_seconds=60), CFG)
    assert level == "low" and score == 0


def test_elevated_amount_alone_is_low():
    score, level, reasons = score_transfer(facts(amount=Decimal("5000"), sender_balance=Decimal("100000")), CFG)
    assert reasons == ["elevated_amount"] and level == "low" and score == 20


def test_large_amount_alone_is_medium():
    score, level, reasons = score_transfer(facts(amount=Decimal("10000"), sender_balance=Decimal("100000")), CFG)
    assert reasons == ["large_amount"] and level == "medium" and score == 40


def test_burst_of_transfers_is_medium_not_declined():
    score, level, reasons = score_transfer(facts(sender_tx_count_1m=12), CFG)
    assert reasons == ["velocity_high"] and level == "medium" and score == 60


def test_new_account_draining_balance_to_new_receiver_is_high():
    score, level, reasons = score_transfer(
        facts(
            amount=Decimal("950"),
            sender_balance=Decimal("1000"),
            sender_account_age_seconds=120,
            first_time_receiver=True,
            sender_tx_count_1m=5,
        ),
        CFG,
    )
    assert level == "high"
    assert set(reasons) == {"velocity_elevated", "balance_drain", "new_account"}
    assert score == 85


def test_large_transfer_over_daily_limit_is_high():
    score, level, reasons = score_transfer(
        facts(amount=Decimal("12000"), sender_balance=Decimal("500000"), sender_amount_24h=Decimal("9000")),
        CFG,
    )
    assert level == "high" and set(reasons) == {"large_amount", "daily_volume"} and score == 70


def test_first_time_receiver_only_counts_for_larger_amounts():
    _, _, small = score_transfer(facts(amount=Decimal("999"), first_time_receiver=True, sender_balance=Decimal("100000")), CFG)
    _, _, large = score_transfer(facts(amount=Decimal("1000"), first_time_receiver=True, sender_balance=Decimal("100000")), CFG)
    assert small == [] and large == ["first_time_receiver"]


def test_score_is_capped_at_100():
    score, level, _ = score_transfer(
        facts(
            amount=Decimal("50000"),
            sender_balance=Decimal("50000"),
            sender_tx_count_1m=50,
            sender_amount_24h=Decimal("100000"),
            sender_account_age_seconds=10,
            first_time_receiver=True,
        ),
        CFG,
    )
    assert score == 100 and level == "high"


def test_missing_optional_facts_do_not_crash():
    score, level, _ = score_transfer(TransferFacts(amount=Decimal("10")), CFG)
    assert (score, level) == (0, "low")


def test_thresholds_are_configurable():
    strict = RuleConfig(high_threshold=40)
    _, level, _ = score_transfer(facts(amount=Decimal("10000"), sender_balance=Decimal("100000")), strict)
    assert level == "high"
