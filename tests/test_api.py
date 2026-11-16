"""Integration tests for FastAPI service endpoints using TestClient."""

import json
import os
from collections.abc import Iterator
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
import psycopg
import pytest

from pipeline.api.main import app, get_db
from pipeline.models import Trade
from pipeline.processing.bars import Bar
from pipeline.processing.detectors import Alert
from pipeline.processing.process import save_alerts, save_bars
from pipeline.storage.repository import PostgresTradeRepository

DB_CONN = os.getenv(
    "TEST_DATABASE_URL",
    "host=localhost port=5432 dbname=crypto_dw user=crypto_user password=crypto_secret_password_2026",
)

TEST_SYMBOL = "TESTAPI"
client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_api_test_data():
    """Remove test rows for TESTAPI before and after each test without touching live BTCUSDT data."""
    def _cleanup() -> None:
        with psycopg.connect(DB_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM raw.trades WHERE symbol = %s;", (TEST_SYMBOL,))
                cur.execute("DELETE FROM ops.bars WHERE symbol = %s;", (TEST_SYMBOL,))
                cur.execute("DELETE FROM ops.alerts WHERE symbol = %s;", (TEST_SYMBOL,))
                cur.execute(
                    "DELETE FROM raw.rejected_events WHERE reason LIKE %s;",
                    (f"[{TEST_SYMBOL}]%",),
                )

    _cleanup()
    yield
    _cleanup()
    app.dependency_overrides.clear()


def test_health_ok():
    """Verify GET /health returns 200 and connected status when DB is reachable."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "database": "connected"}


def test_health_db_unavailable_returns_503():
    """Verify GET /health returns 503 when PostgreSQL raises psycopg.Error."""
    broken_conn = MagicMock()
    broken_cur = MagicMock()
    broken_cur.execute.side_effect = psycopg.OperationalError("connection refused")
    broken_conn.cursor.return_value.__enter__.return_value = broken_cur

    def _override_get_db() -> Iterator[MagicMock]:
        yield broken_conn

    app.dependency_overrides[get_db] = _override_get_db
    resp = client.get("/health")
    assert resp.status_code == 503


def test_get_latest_trades_and_validation():
    """Verify /trades/latest returns newest trades first, [] for empty symbol, and 422 on bad limit."""
    repo = PostgresTradeRepository(DB_CONN)
    repo.insert_many([
        Trade(TEST_SYMBOL, 1, 80000.0, 0.1, 1700000001000, False),
        Trade(TEST_SYMBOL, 2, 80500.0, 0.2, 1700000002000, True),
    ])

    resp = client.get(f"/trades/latest?symbol={TEST_SYMBOL}&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["trade_id"] == 2
    assert data[0]["price"] == 80500.0

    # Empty symbol should return 200 with []
    empty_resp = client.get("/trades/latest?symbol=UNKNOWN_SYM")
    assert empty_resp.status_code == 200
    assert empty_resp.json() == []

    # Invalid limit triggers FastAPI 422
    bad_resp = client.get("/trades/latest?limit=500")
    assert bad_resp.status_code == 422


def test_get_bars_and_validation():
    """Verify /bars returns bars newest-first and enforces parameter bounds."""
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            save_bars(cur, [
                Bar(TEST_SYMBOL, 60, 1700000040000, 80000.0, 80100.0, 79900.0, 80050.0, 1.5, 10),
                Bar(TEST_SYMBOL, 60, 1700000100000, 80050.0, 80200.0, 80000.0, 80150.0, 2.0, 15),
            ])

    resp = client.get(f"/bars?symbol={TEST_SYMBOL}&bar_seconds=60&limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["bar_start_ms"] == 1700000100000
    assert data[1]["bar_start_ms"] == 1700000040000

    assert client.get("/bars?bar_seconds=0").status_code == 422
    assert client.get("/bars?limit=1000").status_code == 422


def test_get_alerts():
    """Verify /alerts returns stored JSONB details and orders by bar_end_ms DESC."""
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            save_alerts(cur, [
                Alert(TEST_SYMBOL, "PRICE_SPIKE", "WARNING", 4.2, 3.0, 1700000040000, 1700000105000),
                Alert(TEST_SYMBOL, "PRICE_PLUNGE", "WARNING", -3.8, 3.0, 1700000100000, 1700000165000),
            ])

    resp = client.get(f"/alerts?symbol={TEST_SYMBOL}&limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["bar_start_ms"] == 1700000100000
    assert data[0]["details_json"]["alert_type"] == "PRICE_PLUNGE"
    assert client.get("/alerts?limit=0").status_code == 422


def test_get_stats_metrics_and_empty_symbol():
    """Verify /stats computes total_trades, error_rate, and p50/p95 latencies accurately."""
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            # Insert 4 trades with deterministic latencies: 10, 20, 30, 100 ms
            base_t = 1700000000000
            for idx, lat in enumerate([10, 20, 30, 100], start=1):
                cur.execute(
                    """
                    INSERT INTO raw.trades
                        (symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (TEST_SYMBOL, idx, 80000.0, 0.1, base_t + idx, False, base_t + idx + lat),
                )
            # Insert 1 rejected event
            cur.execute(
                """
                INSERT INTO raw.rejected_events (received_at_ms, reason, raw_payload)
                VALUES (%s, %s, %s::jsonb);
                """,
                (base_t, f"[{TEST_SYMBOL}] Invalid price", json.dumps({"p": "-1"})),
            )
            cur.execute("SELECT COUNT(*) FROM raw.rejected_events;")
            row = cur.fetchone()
            assert row is not None
            expected_rejected = int(row[0])

    resp = client.get(f"/stats?symbol={TEST_SYMBOL}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["symbol"] == TEST_SYMBOL
    assert data["total_trades"] == 4
    assert data["total_rejected"] == expected_rejected
    expected_error_rate = expected_rejected / (4 + expected_rejected)
    assert data["error_rate"] == pytest.approx(expected_error_rate)
    assert data["p50_latency_ms"] == pytest.approx(25.0)
    assert data["p95_latency_ms"] is not None

    # Empty symbol: percentiles must be None
    empty_stats = client.get("/stats?symbol=NONEXISTENT_SYM")
    assert empty_stats.status_code == 200
    empty_data = empty_stats.json()
    assert empty_data["total_trades"] == 0
    assert empty_data["p50_latency_ms"] is None
    assert empty_data["p95_latency_ms"] is None
