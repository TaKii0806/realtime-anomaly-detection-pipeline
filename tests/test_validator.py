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
    """Trường hợp 1: Dữ liệu chuẩn hợp lệ."""
    trade = parse_trade(VALID_RAW, expected_symbol="BTCUSDT", now_ms=1700000002000)
    assert isinstance(trade, Trade)
    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == 1234567
    assert trade.price == 80000.50
    assert trade.quantity == 0.015
    assert trade.trade_time_ms == 1700000001000
    assert trade.is_buyer_maker is True

def test_parse_trade_not_a_dict():
    """Trường hợp 2: Payload không phải dictionary."""
    with pytest.raises(ValidationError, match="Payload must be a dict"):
        parse_trade("chuỗi văn bản không phải dict")  # type: ignore

def test_parse_trade_missing_field():
    """Trường hợp 3: Thiếu trường bắt buộc (thiếu giá 'p')."""
    bad_raw = dict(VALID_RAW)
    del bad_raw["p"]
    with pytest.raises(ValidationError, match="Missing required fields"):
        parse_trade(bad_raw)

def test_parse_trade_wrong_event_type():
    """Trường hợp 4: Sai loại sự kiện (không phải 'trade')."""
    bad_raw = dict(VALID_RAW, e="kline")
    with pytest.raises(ValidationError, match="Event type must be trade"):
        parse_trade(bad_raw)

def test_parse_trade_symbol_mismatch():
    """Trường hợp 5: Sai symbol (nhận ETHUSDT thay vì BTCUSDT)."""
    bad_raw = dict(VALID_RAW, s="ETHUSDT")
    with pytest.raises(ValidationError, match="Symbol mismatch"):
        parse_trade(bad_raw, expected_symbol="BTCUSDT")

def test_parse_trade_non_numeric_price():
    """Trường hợp 6: Giá không phải số hợp lệ."""
    bad_raw = dict(VALID_RAW, p="abc_khong_phai_so")
    with pytest.raises(ValidationError, match="Invalid number format"):
        parse_trade(bad_raw)

def test_parse_trade_zero_or_negative_price():
    """Trường hợp 7: Giá bằng 0 hoặc âm."""
    bad_raw = dict(VALID_RAW, p="-100.0")
    with pytest.raises(ValidationError, match="Price and quantity must be positive"):
        parse_trade(bad_raw)

def test_parse_trade_nan_price():
    """Trường hợp 8: Giá là NaN."""
    bad_raw = dict(VALID_RAW, p="nan")
    with pytest.raises(ValidationError, match="Price and quantity mustn't be NaN or Inf"):
        parse_trade(bad_raw)

def test_parse_trade_future_timestamp():
    """Trường hợp 9: Thời gian ở tương lai xa hơn 5 giây."""
    bad_raw = dict(VALID_RAW, T=1700000010000)
    with pytest.raises(ValidationError, match="Trade time is in the future"):
        parse_trade(bad_raw, now_ms=1700000001000)
