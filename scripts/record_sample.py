import asyncio
import json
from pathlib import Path
import websockets

STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
OUTPUT_DIR = Path("data/raw")
OUTPUT_FILE = OUTPUT_DIR / "btcusdt_raw.jsonl"

async def record():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    async with websockets.connect(STREAM_URL) as ws:
        print("Kết nối thành công ! Đang đọc giao dịch...")
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            async for message in ws:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                count += 1
                f.write(message + '\n')
                f.flush()
                if count <= 3:
                    print(f"Giao dich {count}: {message}")
                elif count % 50 == 0:
                    print(f"Đã ghi nhận {count} giao dịch")
if __name__ == "__main__":
    try:
        asyncio.run(record())
    except KeyboardInterrupt:
        print("Đã dừng")
