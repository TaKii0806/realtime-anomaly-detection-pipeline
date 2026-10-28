import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence

from pipeline.models import Trade
from pipeline.validator import ValidationError, parse_trade

logger = logging.getLogger(__name__)


class Sink(Protocol):
    """Interface defining storage sinks for processed pipeline items."""

    async def write(self, items: Sequence[Any]) -> None:
        """Persist a batch of items to the underlying storage."""
        ...


class ListSink:
    """In-memory sink storing items in a Python list (primarily used for unit testing)."""

    def __init__(self) -> None:
        self.items: list[Any] = []

    async def write(self, items: Sequence[Any]) -> None:
        self.items.extend(items)


class JsonlSink:
    """Sink that appends serialized JSON records to a .jsonl file."""

    def __init__(self, file_path: Path | str) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    async def write(self, items: Sequence[Any]) -> None:
        if not items:
            return

        def _sync_write() -> None:
            with open(self.file_path, "a", encoding="utf-8") as f:
                for item in items:
                    if is_dataclass(item):
                        line = json.dumps(asdict(item))
                    elif isinstance(item, dict):
                        line = json.dumps(item)
                    else:
                        line = json.dumps(str(item))
                    f.write(line + "\n")

        # Offload blocking file I/O to a worker thread to keep the event loop responsive
        await asyncio.to_thread(_sync_write)


@dataclass
class PipelineStats:
    """Metrics collector for real-time pipeline telemetry."""

    received: int = 0
    valid: int = 0
    rejected: int = 0
    dropped: int = 0
    written: int = 0


async def receiver(
    source: Any,
    queue: asyncio.Queue[Trade],
    rejected_sink: Sink | None,
    stats: PipelineStats,
    symbol: str = "BTCUSDT",
    check_future_time: bool = True,
) -> None:
    """
    Ingest records from the source, validate them, and push valid trades to the queue.

    Unparseable records are diverted to the rejected_sink.
    If the queue reaches capacity, records are dropped to prevent backpressure from blocking sockets.
    """
    stream_iter = source.stream() if hasattr(source, "stream") else source
    async for raw_msg in stream_iter:
        stats.received += 1
        now_ms = int(time.time() * 1000) if check_future_time else None
        try:
            trade = parse_trade(raw_msg, expected_symbol=symbol, now_ms=now_ms)
            stats.valid += 1
        except ValidationError as err:
            stats.rejected += 1
            if rejected_sink is not None:
                await rejected_sink.write([{"reason": str(err), "raw": raw_msg}])
            continue

        try:
            queue.put_nowait(trade)
        except asyncio.QueueFull:
            stats.dropped += 1
            logger.warning("Queue is full; dropping trade_id=%s to avoid socket blocking", trade.trade_id)


async def batcher(
    queue: asyncio.Queue[Trade | None],
    sink: Sink,
    stats: PipelineStats,
    batch_size: int = 500,
    flush_interval_s: float = 1.0,
) -> None:
    """
    Consume items from the queue and flush micro-batches to the sink.

    Flushing is triggered when:
    1. Batch count reaches batch_size.
    2. Elapsed time since last flush exceeds flush_interval_s.
    3. Sentinel None is received or task is cancelled.
    """
    batch: list[Trade] = []
    loop = asyncio.get_running_loop()
    last_flush_time = loop.time()

    async def _flush() -> None:
        nonlocal last_flush_time
        if not batch:
            return
        await sink.write(batch)
        stats.written += len(batch)
        batch.clear()
        last_flush_time = loop.time()

    try:
        while True:
            elapsed = loop.time() - last_flush_time
            remaining_timeout = max(0.0, flush_interval_s - elapsed)

            try:
                item = await asyncio.wait_for(queue.get(), timeout=remaining_timeout)
            except asyncio.TimeoutError:
                await _flush()
                continue

            if item is None:
                queue.task_done()
                await _flush()
                break

            batch.append(item)
            queue.task_done()

            if len(batch) >= batch_size:
                await _flush()
    finally:
        await _flush()