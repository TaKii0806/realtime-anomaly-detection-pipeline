"""Unit tests for OHLCV Time Bar Aggregator."""

import pytest
from pipeline.models import Trade
from pipeline.processing.bars import Bar, build_bars


def _make_trade(
    symbol: str = "BTCUSDT",
    trade_id: int = 1,
    price: float = 80000.0,
    quantity: float = 0.5,
    trade_time_ms: int = 1700000000000,
) -> Trade:
    return Trade(
        symbol=symbol,
        trade_id=trade_id,
        price=price,
        quantity=quantity,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=False,
    )


def test_build_bars_empty_input():
    """Verify that empty trade sequence returns an empty bar list."""
    assert build_bars([]) == []


def test_build_bars_single_trade():
    """Verify that a single trade produces a valid 1-trade bar."""
    trade = _make_trade(price=82000.0, quantity=1.2, trade_time_ms=1699999980000 + 15000)
    bars = build_bars([trade], bar_seconds=60)

    assert len(bars) == 1
    bar = bars[0]
    assert bar.symbol == "BTCUSDT"
    assert bar.bar_seconds == 60
    assert bar.bar_start_ms == 1699999980000  # Correct 60s aligned bucket
    assert bar.open == 82000.0
    assert bar.high == 82000.0
    assert bar.low == 82000.0
    assert bar.close == 82000.0
    assert pytest.approx(bar.volume) == 1.2
    assert bar.trade_count == 1


def test_build_bars_multiple_trades_same_minute():
    """Verify OHLCV calculation across multiple trades within the same bar."""
    t0 = 1699999980000  # Perfectly aligned to 60s
    trades = [
        _make_trade(trade_id=1, price=80000.0, quantity=0.1, trade_time_ms=t0 + 1000),  # Open
        _make_trade(trade_id=2, price=80500.0, quantity=0.2, trade_time_ms=t0 + 10000), # High
        _make_trade(trade_id=3, price=79500.0, quantity=0.3, trade_time_ms=t0 + 30000), # Low
        _make_trade(trade_id=4, price=80200.0, quantity=0.4, trade_time_ms=t0 + 55000), # Close
    ]

    bars = build_bars(trades, bar_seconds=60)

    assert len(bars) == 1
    b = bars[0]
    assert b.open == 80000.0
    assert b.high == 80500.0
    assert b.low == 79500.0
    assert b.close == 80200.0
    assert pytest.approx(b.volume) == 1.0
    assert b.trade_count == 4


def test_build_bars_across_multiple_intervals():
    """Verify that trades spanning multiple minutes are partitioned into separate bars."""
    t0 = 1699999980000  # Perfectly aligned to 60s
    trades = [
        # Minute 0
        _make_trade(trade_id=1, price=80000.0, quantity=0.1, trade_time_ms=t0 + 5000),
        _make_trade(trade_id=2, price=80200.0, quantity=0.2, trade_time_ms=t0 + 40000),
        # Minute 1 (60s later)
        _make_trade(trade_id=3, price=80300.0, quantity=0.3, trade_time_ms=t0 + 65000),
        _make_trade(trade_id=4, price=80100.0, quantity=0.4, trade_time_ms=t0 + 110000),
    ]

    bars = build_bars(trades, bar_seconds=60)

    assert len(bars) == 2
    assert bars[0].bar_start_ms == t0
    assert bars[0].open == 80000.0
    assert bars[0].close == 80200.0

    assert bars[1].bar_start_ms == t0 + 60000
    assert bars[1].open == 80300.0
    assert bars[1].close == 80100.0


def test_build_bars_multiple_symbols():
    """Verify that different symbols in the same time window produce separate bars."""
    t0 = 1700000000000
    trades = [
        _make_trade(symbol="BTCUSDT", trade_id=1, price=80000.0, quantity=0.1, trade_time_ms=t0),
        _make_trade(symbol="ETHUSDT", trade_id=2, price=3000.0, quantity=2.0, trade_time_ms=t0),
    ]

    bars = build_bars(trades, bar_seconds=60)

    assert len(bars) == 2
    symbols = {b.symbol for b in bars}
    assert symbols == {"BTCUSDT", "ETHUSDT"}
