import pytest
from pipeline.models import Trade
from pipeline.validator import ValidationError, parse_trade

VALID_RAW = {
    "e": "trade",
    "E": 1700000002000,
    "s": "BTCUSDT",
    "t": 1234567,
    "p": "80000.50",
    "q": "0.01500000",
    "T": 1700000001000,
    "m": True,
    "M": True,
}


def test_parse_trade_valid():
    """Case 1: Valid standard trade payload."""
    trade = parse_trade(VALID_RAW, expected_symbol="BTCUSDT", now_ms=1700000002000)
    assert isinstance(trade, Trade)
    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == 1234567
    assert trade.price == 80000.50
    assert trade.quantity == 0.015
    assert trade.trade_time_ms == 1700000001000
    assert trade.is_buyer_maker is True


def test_parse_trade_not_a_dict():
    """Case 2: Payload is not a dictionary."""
    with pytest.raises(ValidationError, match="Payload must be a dict"):
        parse_trade("non_dict_string")  # type: ignore


def test_parse_trade_missing_field():
    """Case 3: Missing required field ('p' missing)."""
    bad_raw = dict(VALID_RAW)
    del bad_raw["p"]
    with pytest.raises(ValidationError, match="Missing required fields"):
        parse_trade(bad_raw)


def test_parse_trade_wrong_event_type():
    """Case 4: Invalid event type (not 'trade')."""
    bad_raw = dict(VALID_RAW, e="kline")
    with pytest.raises(ValidationError, match="Event type must be trade"):
        parse_trade(bad_raw)


def test_parse_trade_symbol_mismatch():
    """Case 5: Trading pair symbol mismatch."""
    bad_raw = dict(VALID_RAW, s="ETHUSDT")
    with pytest.raises(ValidationError, match="Symbol mismatch"):
        parse_trade(bad_raw, expected_symbol="BTCUSDT")


def test_parse_trade_non_numeric_price():
    """Case 6: Non-numeric price string."""
    bad_raw = dict(VALID_RAW, p="not_a_number")
    with pytest.raises(ValidationError, match="Invalid number format"):
        parse_trade(bad_raw)


def test_parse_trade_zero_or_negative_price():
    """Case 7: Non-positive price."""
    bad_raw = dict(VALID_RAW, p="-100.0")
    with pytest.raises(ValidationError, match="Price and quantity must be positive"):
        parse_trade(bad_raw)


def test_parse_trade_nan_price():
    """Case 8: NaN price."""
    bad_raw = dict(VALID_RAW, p="nan")
    with pytest.raises(ValidationError, match="Price and quantity mustn't be NaN or Inf"):
        parse_trade(bad_raw)


def test_parse_trade_future_timestamp():
    """Case 9: Timestamp too far in the future (>5 seconds)."""
    bad_raw = dict(VALID_RAW, T=1700000010000)
    with pytest.raises(ValidationError, match="Trade time is in the future"):
        parse_trade(bad_raw, now_ms=1700000001000)
