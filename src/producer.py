import time
import requests
from datetime import datetime
from database import insert_transaction

URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

print("Đang lấy dữ liệu...")

while True:
    try:
        response = requests.get(URL, timeout=5)
        data = response.json()
        print(data)
        symbol = data['symbol']
        price = float(data["price"])
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_transaction(symbol, price, timestamp)
        print(f"Đã lưu: {symbol} | {price} | {timestamp}")
    except Exception as e:
        print("Lỗi kết nối:", e)

    time.sleep(2)