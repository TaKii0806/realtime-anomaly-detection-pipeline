import asyncio
import json
from pathlib import Path
from typing import AsyncIterator
import random

class ReplaySource:
    # Nguồn phát lại dữ liệu từ file JSONL trên đĩa cứng
    def __init__(self, file_path:str | Path, speed:float=0.0):
        self.file_path = Path(file_path)
        self.speed = speed
    
    async def stream(self) -> AsyncIterator[dict]:
        with open(self.file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                lines = json.loads(line)
                if (self.speed > 0):
                    await asyncio.sleep(self.speed)
                yield lines

class BinanceWebSocketSource:
    # Nguồn nhận luồng giao dịch real-time từ Binance WebSocket + auto reconnect
    def __init__ (
        self,
        url:str="wss://stream.binance.com:9443/ws/btcusdt@trade",
        max_retries:int=5,
        initial_backoff_s:float=1.0,
        max_backoff_s:float=30.0
    ):
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
                print(f"Đang kết nối tới {self.url}...")
                async with websockets.connect(self.url) as ws:
                    print("Kết nối thành công! Đang ghi lại...")

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
                    print(f"Đã thử {retries} lần nhưng thất bại. Dừng kết nối: {e}")
                    raise

                jitter = random.uniform(0, 0.5)
                sleep_time = min(back_off, self.max_backoff_s) + jitter
                print(f"Mất kết nối ({e}), thử lại lần {retries}/{self.max_retries} sau {sleep_time:.2f}s...")
                await asyncio.sleep(sleep_time)
                back_off *= 2