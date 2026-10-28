import asyncio
import json
import logging
import random
from pathlib import Path
from typing import AsyncIterator

logger = logging.getLogger(__name__)


class ReplaySource:
    """Replays raw JSONL trade records from local disk."""

    def __init__(self, file_path: str | Path, speed: float = 0.0) -> None:
        self.file_path = Path(file_path)
        self.speed = speed

    async def stream(self) -> AsyncIterator[dict]:
        with open(self.file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if self.speed > 0:
                    await asyncio.sleep(self.speed)
                yield record


class BinanceWebSocketSource:
    """Streams live trade data from Binance WebSocket with exponential backoff and jitter."""

    def __init__(
        self,
        url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade",
        max_retries: int = 5,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
    ) -> None:
        self.url = url
        self.max_retries = max_retries
        self.initial_backoff_s = initial_backoff_s
        self.max_backoff_s = max_backoff_s

    async def stream(self) -> AsyncIterator[dict]:
        import websockets

        retries = 0
        back_off = self.initial_backoff_s
        while retries < self.max_retries:
            try:
                logger.info("Connecting to %s...", self.url)
                async with websockets.connect(self.url) as ws:
                    logger.info("Connected successfully. Streaming trade events...")
                    retries = 0
                    back_off = self.initial_backoff_s

                    async for message in ws:
                        if isinstance(message, bytes):
                            message = message.decode("utf-8")
                        data = json.loads(message)
                        yield data
            except Exception as e:
                retries += 1
                if retries >= self.max_retries:
                    logger.error("Connection failed after %d retries: %s", retries, e)
                    raise

                jitter = random.uniform(0, 0.5)
                sleep_time = min(back_off, self.max_backoff_s) + jitter
                logger.warning(
                    "Connection lost (%s). Retrying %d/%d after %.2fs...",
                    e,
                    retries,
                    self.max_retries,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)
                back_off *= 2