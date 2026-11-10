"""Unit tests for RollingZScoreDetector."""

import pytest
from pipeline.processing.bars import Bar
from pipeline.processing.detectors import Alert, RollingZScoreDetector


def _make_bar(
    symbol: str = "BTCUSDT",
    close: float = 80000.0,
    bar_start_ms: int = 1700000000000,
) -> Bar:
    return Bar(
        symbol=symbol,
        bar_seconds=60,
        bar_start_ms=bar_start_ms,
        open=close,
        high=close + 10.0,
        low=close - 10.0,
        close=close,
        volume=10.0,
        trade_count=100,
    )


def test_detector_warmup_period():
    """Verify that detector does not emit alerts during warmup, but history grows."""
    detector = RollingZScoreDetector(window_size=20, threshold=3.0, min_periods=5)

    # Feed 4 bars (< min_periods=5)
    for i in range(4):
        alert = detector.process(_make_bar(close=80000.0 + i, bar_start_ms=1000 * i))
        assert alert is None

    # Check that history actually accumulated 4 bars
    assert len(detector.windows["BTCUSDT"]) == 4


def test_detector_detects_price_spike():
    """Verify that a sudden price surge triggers PRICE_SPIKE alert."""
    detector = RollingZScoreDetector(window_size=20, threshold=3.0, min_periods=5)

    # Feed 10 steady bars around 80,000
    for i in range(10):
        detector.process(_make_bar(close=80000.0 + (i % 2) * 10, bar_start_ms=1000 * i))

    # Bar 11 spikes to 95,000
    spike_bar = _make_bar(close=95000.0, bar_start_ms=11000)
    alert = detector.process(spike_bar)

    assert alert is not None
    assert alert.alert_type == "PRICE_SPIKE"
    assert alert.metric_value > 3.0
    assert alert.symbol == "BTCUSDT"


def test_detector_detects_price_plunge():
    """Verify that a sudden crash triggers PRICE_PLUNGE alert."""
    detector = RollingZScoreDetector(window_size=20, threshold=3.0, min_periods=5)

    for i in range(10):
        detector.process(_make_bar(close=80000.0, bar_start_ms=1000 * i))

    # Crash to 60,000
    crash_bar = _make_bar(close=60000.0, bar_start_ms=11000)
    alert = detector.process(crash_bar)

    assert alert is not None
    assert alert.alert_type == "PRICE_PLUNGE"
    assert alert.metric_value < -3.0

