"""Unit tests for KafkaTradeSink, KafkaDlqSink, and ingest --sink kafka routing."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from pipeline.ingest import run_pipeline
from pipeline.models import Trade
from pipeline.streaming.kafka_sink import KafkaDlqSink, KafkaTradeSink


def _make_fake_producer() -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = 0
    return producer


def test_kafka_trade_sink_empty_batch():
    """Verify empty batch skips produce and flush calls."""
    producer = _make_fake_producer()
    sink = KafkaTradeSink(producer=producer)

    asyncio.run(sink.write([]))

    producer.produce.assert_not_called()
    producer.flush.assert_not_called()


def test_kafka_trade_sink_produces_json_with_symbol_key():
    """Verify KafkaTradeSink serializes Trade to UTF-8 JSON and uses symbol as key."""
    producer = _make_fake_producer()
    sink = KafkaTradeSink(topic="trades.raw", producer=producer)
    trades = [
        Trade("BTCUSDT", 101, 85000.5, 0.25, 1700000001000, False),
        Trade("ETHUSDT", 102, 3200.0, 1.5, 1700000002000, True),
    ]

    asyncio.run(sink.write(trades))

    assert producer.produce.call_count == 2
    first_kwargs = producer.produce.call_args_list[0].kwargs
    assert first_kwargs["topic"] == "trades.raw"
    assert first_kwargs["key"] == b"BTCUSDT"
    assert json.loads(first_kwargs["value"].decode("utf-8")) == {
        "symbol": "BTCUSDT",
        "trade_id": 101,
        "price": 85000.5,
        "quantity": 0.25,
        "trade_time_ms": 1700000001000,
        "is_buyer_maker": False,
    }
    assert callable(first_kwargs["on_delivery"])
    assert producer.poll.call_count == 2
    producer.flush.assert_called_once_with(timeout=10.0)


def test_kafka_trade_sink_retries_on_buffer_error():
    """Verify BufferError triggers poll(0.5) backpressure wait and retries produce."""
    producer = _make_fake_producer()
    producer.produce.side_effect = [BufferError("Local: Queue full"), None]
    sink = KafkaTradeSink(producer=producer)
    trade = Trade("BTCUSDT", 1, 80000.0, 0.1, 1700000000000, False)

    asyncio.run(sink.write([trade]))

    assert producer.produce.call_count == 2
    producer.poll.assert_any_call(0.5)
    producer.flush.assert_called_once_with(timeout=10.0)


def test_kafka_dlq_sink_handles_missing_and_non_str_symbol():
    """Verify KafkaDlqSink safely handles missing or non-string symbol in malformed payloads."""
    producer = _make_fake_producer()
    dlq = KafkaDlqSink(topic="trades.dlq", producer=producer)
    items = [
        {"reason": "Negative price", "raw": {"s": "BTCUSDT", "p": "-1"}},
        {"reason": "Numeric symbol", "raw": {"s": 12345}},
        {"reason": "Empty symbol", "raw": {"s": ""}},
        {"reason": "Plain text", "raw": "CORRUPTED_LINE"},
    ]

    asyncio.run(dlq.write(items))

    assert producer.produce.call_count == 4
    keys = [call.kwargs["key"] for call in producer.produce.call_args_list]
    assert keys == [b"BTCUSDT", None, None, None]
    topics = [call.kwargs["topic"] for call in producer.produce.call_args_list]
    assert all(t == "trades.dlq" for t in topics)
    producer.flush.assert_called_once_with(timeout=10.0)


def test_run_pipeline_kafka_sink_without_database_url(monkeypatch, tmp_path):
    """Verify ingest --sink kafka runs without DATABASE_URL and routes valid/invalid records."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    sample_file = tmp_path / "mini.jsonl"
    sample_file.write_text(
        "\n".join([
            json.dumps({"e": "trade", "s": "BTCUSDT", "t": 1, "p": "85000", "q": "0.1", "T": 1700000000000, "m": False}),
            json.dumps({"e": "trade", "s": "BTCUSDT", "t": 2, "p": "-5", "q": "0.1", "T": 1700000001000, "m": True}),
        ]),
        encoding="utf-8",
    )

    fake_producer = _make_fake_producer()
    with patch("pipeline.ingest.Producer", return_value=fake_producer):
        asyncio.run(
            run_pipeline(
                source_type="replay",
                file_path=str(sample_file),
                speed=0.0,
                sink_type="kafka",
            )
        )

    topics = [call.kwargs["topic"] for call in fake_producer.produce.call_args_list]
    assert sorted(topics) == ["trades.dlq", "trades.raw"]


def test_run_pipeline_invalid_sink_raises_value_error():
    """Verify unsupported sink_type fails loudly with ValueError."""
    with pytest.raises(ValueError, match="postgres|kafka"):
        asyncio.run(run_pipeline(source_type="replay", sink_type="sqlite"))
