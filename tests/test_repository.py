"""Integration tests for PostgresTradeRepository."""

import os
import pytest
from pipeline.models import Trade
from pipeline.storage.repository import PostgresTradeRepository

# Connection info for PostgreSQL container running locally
DB_CONN = os.getenv(
    "TEST_DATABASE_URL",
    "host=localhost port=5432 dbname=crypto_dw user=crypto_user password=crypto_secret_password_2026",
)


def _make_trade(trade_id: int, price: float = 85000.0) -> Trade:
    return Trade(
        symbol="BTCUSDT",
        trade_id=trade_id,
        price=price,
        quantity=0.05,
        trade_time_ms=1700000000000 + trade_id,
        is_buyer_maker=False,
    )


@pytest.fixture(autouse=True)
def clean_trades_table():
    """Clean up raw.trades table before and after each test."""
    import psycopg

    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.trades WHERE symbol = 'BTCUSDT'")

    yield

    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.trades WHERE symbol = 'BTCUSDT'")


def test_insert_many_inserts_records():
    """Verify that insert_many inserts valid trades and count reflects them."""
    repo = PostgresTradeRepository(DB_CONN)
    trades = [_make_trade(1), _make_trade(2), _make_trade(3)]

    inserted = repo.insert_many(trades)

    assert inserted == 3
    assert repo.count("BTCUSDT") == 3


def test_insert_many_is_idempotent():
    """Verify that re-inserting identical trades does not duplicate rows (ON CONFLICT DO NOTHING)."""
    repo = PostgresTradeRepository(DB_CONN)
    trades = [_make_trade(10), _make_trade(11)]

    # First insert: 2 rows inserted
    inserted_1 = repo.insert_many(trades)
    assert inserted_1 == 2
    assert repo.count("BTCUSDT") == 2

    # Second insert with exact same trades: 0 rows inserted, count remains 2
    inserted_2 = repo.insert_many(trades)
    assert inserted_2 == 0
    assert repo.count("BTCUSDT") == 2


def test_insert_many_empty_list():
    """Verify that passing an empty sequence returns 0 without database query."""
    repo = PostgresTradeRepository(DB_CONN)
    assert repo.insert_many([]) == 0


def test_fetch_since_returns_ordered_trades():
    """Verify that fetch_since retrieves trades filtered by time."""
    repo = PostgresTradeRepository(DB_CONN)
    t1 = _make_trade(1, 85000.0)
    t2 = _make_trade(2, 85100.0)
    repo.insert_many([t1, t2])

    trades = repo.fetch_since("BTCUSDT", since_ms=1700000000002)
    assert len(trades) == 1
    assert trades[0].trade_id == 2


def test_postgres_sink_writes_batches():
    """Verify that PostgresSink correctly offloads writes to thread pool."""
    import asyncio
    from pipeline.storage.repository import PostgresSink

    repo = PostgresTradeRepository(DB_CONN)
    sink = PostgresSink(repo)
    trades = [_make_trade(20), _make_trade(21)]

    asyncio.run(sink.write(trades))
    assert repo.count("BTCUSDT") == 2



