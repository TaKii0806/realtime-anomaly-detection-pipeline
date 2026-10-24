from database import insert_transactions
import time
import requests
from datetime import datetime

SYMBOL = "BTCUSDT"
URL = f"https://api.binance.com/api/v3/trades?symbol={SYMBOL}&limit=5"
POLL_INTERVAL = 2

print(f"Đang lấy dữ liệu({SYMBOL})...")

try:
    while True:
        try:
            response = requests.get(URL, timeout=5)
            trades = response.json()

            buffer = []
            for trade in trades:
                price = float(trade['price'])
                timestamp = datetime.fromtimestamp(trade['time'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                volume = float(trade['qty'])

                buffer.append((SYMBOL, price, volume, timestamp))
            if buffer:
                insert_transactions(buffer)
                print(f"[BATCH] Đã nạp {len(buffer)} giao dịch | Giá mới nhất: {buffer[-1][1]:,.2f} | Vol: {buffer[-1][2]:.4f}")
        except Exception as e:
            print("Lỗi kết nối:", e)
        time.sleep(POLL_INTERVAL)
except KeyboardInterrupt:
    print("Đã dừng Producer")