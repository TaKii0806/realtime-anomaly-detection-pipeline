import asyncio
from pipeline.models import Trade
from pipeline.runner import ListSink, PipelineStats, batcher, receiver


def _make_trade(trade_id: int) -> Trade:
    """Helper to generate a valid Trade instance for testing."""
    return Trade(
        symbol="BTCUSDT",
        trade_id=trade_id,
        price=80000.0,
        quantity=0.01,
        trade_time_ms=1700000000000,
        is_buyer_maker=False,
    )


def test_batcher_flushes_by_batch_size():
    """Verify that batcher automatically flushes when batch_size threshold is reached."""
    async def _run():
        queue: asyncio.Queue[Trade | None] = asyncio.Queue()
        sink = ListSink()
        stats = PipelineStats()

        task = asyncio.create_task(batcher(queue, sink, stats, batch_size=3, flush_interval_s=10.0))

        for i in range(1, 4):
            await queue.put(_make_trade(i))

        await asyncio.sleep(0.05)
        assert len(sink.items) == 3
        assert stats.written == 3

        await queue.put(None)
        await task

    asyncio.run(_run())


def test_batcher_flushes_by_timeout():
    """Verify that batcher flushes accumulated items when flush_interval_s expires."""
    async def _run():
        queue: asyncio.Queue[Trade | None] = asyncio.Queue()
        sink = ListSink()
        stats = PipelineStats()

        task = asyncio.create_task(batcher(queue, sink, stats, batch_size=10, flush_interval_s=0.1))

        await queue.put(_make_trade(1))
        await queue.put(_make_trade(2))

        # Items should remain buffered before timeout
        await asyncio.sleep(0.02)
        assert len(sink.items) == 0

        # Wait past flush interval to trigger timeout flush
        await asyncio.sleep(0.15)
        assert len(sink.items) == 2
        assert stats.written == 2

        await queue.put(None)
        await task

    asyncio.run(_run())


def test_receiver_routes_invalid_to_rejected_sink():
    """Verify that receiver passes valid items to queue and routes invalid items to rejected_sink."""
    async def _mock_source():
        yield {"e": "trade", "s": "BTCUSDT", "t": 101, "p": "80000.0", "q": "0.1", "T": 1700000000000, "m": True}
        yield {"e": "trade", "s": "BTCUSDT", "t": 102}  # Missing required fields
        yield {"e": "trade", "s": "BTCUSDT", "t": 103, "p": "80010.0", "q": "0.2", "T": 1700000000000, "m": False}

    async def _run():
        queue: asyncio.Queue[Trade] = asyncio.Queue()
        rejected_sink = ListSink()
        stats = PipelineStats()

        await receiver(_mock_source(), queue, rejected_sink, stats, check_future_time=False)

        assert stats.received == 3
        assert stats.valid == 2
        assert stats.rejected == 1
        assert queue.qsize() == 2
        assert len(rejected_sink.items) == 1
        assert "Missing required fields" in rejected_sink.items[0]["reason"]

    asyncio.run(_run())


def test_receiver_drops_when_queue_full():
    """Verify that receiver drops records without blocking when queue reaches capacity."""
    async def _mock_source():
        for i in range(1, 5):
            yield {"e": "trade", "s": "BTCUSDT", "t": i, "p": "80000.0", "q": "0.1", "T": 1700000000000, "m": True}

    async def _run():
        queue: asyncio.Queue[Trade] = asyncio.Queue(maxsize=2)
        stats = PipelineStats()

        await receiver(_mock_source(), queue, rejected_sink=None, stats=stats, check_future_time=False)

        assert stats.received == 4
        assert stats.valid == 4
        assert queue.qsize() == 2
        assert stats.dropped == 2

    asyncio.run(_run())
