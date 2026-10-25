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
            if len(data) == 0:
                print(f"Đang đợi giao dịch mới, xin vui lỏng chờ...")
                time.sleep(2)
                continue
            
            for x in data:
                if(len(rows) < 20):
                    print(f'Đang tích lũy dữ liệu [{len(rows) / 20}]')
                    rows.append(x)
                    last_id = x[0]
                    continue
                prices = [r[2] for r in rows]
                mean_price = np.mean(prices)
                std_price = np.std(prices)
                rows.append(x)
                if std_price == 0:
                    z_score = 0.0
                else:
                    z_score = (x[2] - mean_price) / std_price
                is_anomaly = abs(z_score) >= threshold
                print(f"""
                Thời gian: {x[4]} | Coin: {x[1]}
                Giá hiện tại: {x[2]:.2f} | Giá TB (50 lệnh): {mean_price:.2f}
                Độ lệch chuẩn: {std_price:.4f} | Z-Score: {z_score:.2f}
                """)
                if is_anomaly:
                    print(f"⚠️ [CẢNH BÁO] Phát hiện bất thường! Giá lệch {z_score:.2f} lần độ lệch chuẩn.")
                else:
                    print("✅ Trạng thái: Bình thường.")
                last_id = x[0]

        except Exception as e:
            print(f"Lỗi phân tích: {e}")
        time.sleep(3)

if __name__ == "__main__":
    detect_anomaly()