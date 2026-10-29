import argparse
import asyncio
import logging
import sys
import os

from dotenv import load_dotenv
from pipeline.runner import JsonlSink, PipelineStats, batcher, receiver
from pipeline.sources import BinanceWebSocketSource, ReplaySource
from pipeline.storage.repository import PostgresSink, PostgresTradeRepository, PostgresRejectedSink

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline.ingest")


async def telemetry_reporter(stats: PipelineStats, interval_s: float = 10.0) -> None:
    """Periodically logs pipeline throughput and queue telemetry."""
    while True:
        await asyncio.sleep(interval_s)
        logger.info(
            "[TELEMETRY] Received: %d | Valid: %d | Rejected: %d | Dropped: %d | Written: %d",
            stats.received,
            stats.valid,
            stats.rejected,
            stats.dropped,
            stats.written,
        )


async def run_pipeline(
    source_type: str,
    file_path: str = "data/raw/btcusdt_raw.jsonl",
    speed: float = 1.0,
    symbol: str = "BTCUSDT",
    batch_size: int = 500,
    flush_interval_s: float = 1.0,
    minutes: float | None = None,
) -> None:
    """
    Orchestrates the ingestion pipeline: connects source to receiver,
    buffers through a bounded queue, flushes batches via batcher to sinks,
    and guarantees a clean shutdown on completion or cancellation.
    """
    DB_CONN = os.getenv('DATABASE_URL')
    if not DB_CONN:
        raise ValueError ("DATABASE_URL is not set. Please define it in your .env file or system environment.")
    # 1. Initialize data source
    if source_type == "binance":
        source = BinanceWebSocketSource()
        check_future_time = True
    elif source_type == "replay":
        source = ReplaySource(file_path=file_path, speed=speed)
        check_future_time = False
    else:
        raise ValueError(f"Unknown source type: {source_type}")

    # 2. Initialize sinks, shared queue, and metrics collector
    repo = PostgresTradeRepository(DB_CONN)
    sink = PostgresSink(repo)
    rejected_sink = PostgresRejectedSink(repo)
    queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
    stats = PipelineStats()

    logger.info("Starting pipeline (source=%s, symbol=%s)...", source_type, symbol)

    # 3. Launch concurrent worker tasks on event loop
    receiver_task = asyncio.create_task(
        receiver(source, queue, rejected_sink, stats, symbol, check_future_time)
    )
    batcher_task = asyncio.create_task(
        batcher(queue, sink, stats, batch_size, flush_interval_s)
    )
    reporter_task = asyncio.create_task(telemetry_reporter(stats, interval_s=10.0))

    try:
        if minutes is not None and minutes > 0:
            logger.info("Pipeline will run for %.2f minutes...", minutes)
            await asyncio.wait_for(receiver_task, timeout=minutes * 60.0)
        else:
            logger.info("Pipeline running indefinitely. Press Ctrl+C to stop.")
            await receiver_task
    except asyncio.TimeoutError:
        logger.info("Runtime duration reached. Initiating graceful shutdown...")
    except asyncio.CancelledError:
        logger.info("Pipeline cancelled. Initiating graceful shutdown...")
    finally:
        # Graceful shutdown protocol
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass

        if not receiver_task.done():
            receiver_task.cancel()
            try:
                await receiver_task
            except asyncio.CancelledError:
                pass

        # Send sentinel None to flush buffered records in batcher
        await queue.put(None)
        await batcher_task

        logger.info(
            "Pipeline finished. Final stats: Received=%d, Valid=%d, Rejected=%d, Dropped=%d, Written=%d",
            stats.received,
            stats.valid,
            stats.rejected,
            stats.dropped,
            stats.written,
        )


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments for pipeline execution."""
    parser = argparse.ArgumentParser(description="Real-time Trade Ingestion Pipeline")
    parser.add_argument(
        "--source",
        choices=["binance", "replay"],
        default="binance",
        help="Data source: binance (live stream) or replay (file replay)",
    )
    parser.add_argument(
        "--file",
        type=str,
        default="data/raw/btcusdt_raw.jsonl",
        help="Path to JSONL file when using replay source",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay playback speed factor (0 = maximum speed)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTCUSDT",
        help="Trading pair symbol to filter (default: BTCUSDT)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Micro-batch size for sink writes (default: 500)",
    )
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=1.0,
        help="Maximum time in seconds between sink flushes (default: 1.0)",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=None,
        help="Optional auto-stop runtime in minutes",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    try:
        asyncio.run(
            run_pipeline(
                source_type=args.source,
                file_path=args.file,
                speed=args.speed,
                symbol=args.symbol,
                batch_size=args.batch_size,
                flush_interval_s=args.flush_interval,
                minutes=args.minutes,
            )
        )
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Clean shutdown complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()