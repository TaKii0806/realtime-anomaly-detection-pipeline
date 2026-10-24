import pandas as pd
import time
from database import get_recent_transaction, get_latest_id, latest_50_transactions
from collections import deque
import numpy as np

def detect_anomaly(window_size: int = 50, threshold: float = 2.0):
    last_id = get_latest_id()
    rows = deque(latest_50_transactions(), maxlen=window_size)
    while True:
        try:
            data = get_recent_transaction(last_id, limit = window_size)
            if len(data) < 5:
                print(f"Chưa đủ dữ liệu [{len(rows)} / 5]")
                time.sleep(2)
                continue
            
            for x in data:
                rows.append(x)
            columns = ['id', 'symbol', 'price', 'volume', 'timestamp']
            df = pd.DataFrame(rows, columns=columns)
            mean_price = df["price"].mean()
            std_price = df["price"].std()
            latest_record = df.iloc[-1]
            last_id = latest_record['id']
            latest_price = latest_record['price']
            if std_price == 0 or pd.isna(std_price):
                z_score = 0.0
            else:
                z_score = (latest_price - mean_price) / std_price
            
            is_anomaly = abs(z_score) >= threshold
            print(f"""
            Thời gian: {latest_record['timestamp']} | Coin: {latest_record['symbol']}
            Giá hiện tại: {latest_price:.2f} | Giá TB (50 lệnh): {mean_price:.2f}
            Độ lệch chuẩn: {std_price:.4f} | Z-Score: {z_score:.2f}
            """)
            if is_anomaly:
                print(f"⚠️ [CẢNH BÁO] Phát hiện bất thường! Giá lệch {z_score:.2f} lần độ lệch chuẩn.")
            else:
                print("✅ Trạng thái: Bình thường.")
        except Exception as e:
            print(f"Lỗi phân tích: {e}")
        time.sleep(3)

if __name__ == "__main__":
    detect_anomaly()