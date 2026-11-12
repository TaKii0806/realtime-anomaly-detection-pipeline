"""Integration tests for stream processing worker and transactional checkpointing."""

import os
from unittest.mock import patch
import psycopg
import pytest

from pipeline.models import Trade
from pipeline.processing.detectors import RollingZScoreDetector
from pipeline.processing.process import get_checkpoint, run_processing_cycle
from pipeline.storage.repository import PostgresTradeRepository

DB_CONN = os.getenv(
    "TEST_DATABASE_URL",
    "host=localhost port=5432 dbname=crypto_dw user=crypto_user password=crypto_secret_password_2026",
)

TEST_SYMBOL = "TESTBTC"
TEST_CHECKPOINT = "test_bars_1m_worker"
BASE_MINUTE_MS = (1700000000000 // 60000) * 60000  # Exact 1-minute UTC boundary


def _make_trade(trade_id: int, price: float, trade_time_ms: int) -> Trade:
    return Trade(
        symbol=TEST_SYMBOL,
        trade_id=trade_id,
        price=price,
        quantity=0.1,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=False,
    )


@pytest.fixture(autouse=True)
def clean_test_tables():
    """Clean up test records across raw and ops schemas before and after each test."""
    def _cleanup() -> None:
        with psycopg.connect(DB_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM raw.trades WHERE symbol = %s;", (TEST_SYMBOL,))
                cur.execute("DELETE FROM ops.bars WHERE symbol = %s;", (TEST_SYMBOL,))
                cur.execute("DELETE FROM ops.alerts WHERE symbol = %s;", (TEST_SYMBOL,))
                cur.execute("DELETE FROM ops.checkpoints WHERE name = %s;", (TEST_CHECKPOINT,))

    _cleanup()
    yield
    _cleanup()


def test_process_empty_database():
    """Verify worker returns zero counts gracefully when no trades exist for the symbol."""
    detector = RollingZScoreDetector(window_size=10, threshold=3.0, min_periods=5)

    result = run_processing_cycle(
        DB_CONN,
        detector,
        symbol=TEST_SYMBOL,
        checkpoint_name=TEST_CHECKPOINT,
        now_ms=BASE_MINUTE_MS + 120_000,
    )

    assert result == {"bars": 0, "alerts": 0}


def test_process_respects_watermark_and_grace_period():
    """Verify bars are not closed until now_ms passes bar_end + grace_period_ms."""
    repo = PostgresTradeRepository(DB_CONN)
    # Trade inside minute 0: [BASE_MINUTE_MS, BASE_MINUTE_MS + 60_000)
    repo.insert_many([
        _make_trade(1, 80000.0, BASE_MINUTE_MS + 10_000),
        _make_trade(2, 80100.0, BASE_MINUTE_MS + 50_000),
        # Trade inside minute 1 (still open)
        _make_trade(3, 80200.0, BASE_MINUTE_MS + 61_000),
    ])

    detector = RollingZScoreDetector(window_size=10, threshold=3.0, min_periods=5)

    # At +62s with 5s grace period -> watermark is +57s -> minute 0 NOT safe yet
    res_early = run_processing_cycle(
        DB_CONN,
        detector,
        symbol=TEST_SYMBOL,
        bar_seconds=60,
        grace_period_ms=5000,
        checkpoint_name=TEST_CHECKPOINT,
        now_ms=BASE_MINUTE_MS + 62_000,
    )
    assert res_early == {"bars": 0, "alerts": 0}

    # At +66s with 5s grace period -> watermark is +61s -> minute 0 IS safely closed
    res_closed = run_processing_cycle(
        DB_CONN,
        detector,
        symbol=TEST_SYMBOL,
        bar_seconds=60,
        grace_period_ms=5000,
        checkpoint_name=TEST_CHECKPOINT,
        now_ms=BASE_MINUTE_MS + 66_000,
    )
    assert res_closed["bars"] == 1
    assert res_closed["alerts"] == 0
    assert res_closed["new_checkpoint"] == BASE_MINUTE_MS + 60_000


def test_process_idempotency_and_alert_generation():
    """Verify bars and alerts are persisted once and repeated runs do not duplicate data."""
    repo = PostgresTradeRepository(DB_CONN)
    trades = []
    # 5 steady minutes around 80,000
    for i in range(5):
        t_ms = BASE_MINUTE_MS + i * 60_000 + 1000
        trades.append(_make_trade(i + 1, 80000.0 + (i % 2) * 10.0, t_ms))
    # Minute 5: huge spike to 95,000
    trades.append(_make_trade(100, 95000.0, BASE_MINUTE_MS + 5 * 60_000 + 1000))
    repo.insert_many(trades)

    detector = RollingZScoreDetector(window_size=10, threshold=3.0, min_periods=5)
    run_time_ms = BASE_MINUTE_MS + 6 * 60_000 + 10_000

    # First run: processes 6 closed bars and fires 1 alert
    res_1 = run_processing_cycle(
        DB_CONN,
        detector,
        symbol=TEST_SYMBOL,
        bar_seconds=60,
        grace_period_ms=5000,
        checkpoint_name=TEST_CHECKPOINT,
        now_ms=run_time_ms,
    )
    assert res_1["bars"] == 6
    assert res_1["alerts"] == 1
    assert res_1["new_checkpoint"] == BASE_MINUTE_MS + 6 * 60_000

    # Second run at same timestamp: checkpoint already caught up -> 0 work
    res_2 = run_processing_cycle(
        DB_CONN,
        detector,
        symbol=TEST_SYMBOL,
        bar_seconds=60,
        grace_period_ms=5000,
        checkpoint_name=TEST_CHECKPOINT,
        now_ms=run_time_ms,
    )
    assert res_2 == {"bars": 0, "alerts": 0}

    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ops.bars WHERE symbol = %s;", (TEST_SYMBOL,))
            bars_row = cur.fetchone()
            assert bars_row is not None and bars_row[0] == 6
            cur.execute("SELECT COUNT(*) FROM ops.alerts WHERE symbol = %s;", (TEST_SYMBOL,))
            alerts_row = cur.fetchone()
            assert alerts_row is not None and alerts_row[0] == 1


def test_process_transaction_rollback_on_failure():
    """Verify database transaction atomicity: failure rolls back bars and checkpoint."""
    repo = PostgresTradeRepository(DB_CONN)
    repo.insert_many([
        _make_trade(1, 80000.0, BASE_MINUTE_MS + 5000),
    ])

    detector = RollingZScoreDetector(window_size=10, threshold=3.0, min_periods=5)

    with patch("pipeline.processing.process.save_alerts", side_effect=RuntimeError("Simulated DB crash")):
        with pytest.raises(RuntimeError, match="Simulated DB crash"):
            run_processing_cycle(
                DB_CONN,
                detector,
                symbol=TEST_SYMBOL,
                bar_seconds=60,
                grace_period_ms=5000,
                checkpoint_name=TEST_CHECKPOINT,
                now_ms=BASE_MINUTE_MS + 70_000,
            )

    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ops.bars WHERE symbol = %s;", (TEST_SYMBOL,))
            bars_row = cur.fetchone()
            assert bars_row is not None and bars_row[0] == 0
            assert get_checkpoint(cur, TEST_CHECKPOINT) == 0

