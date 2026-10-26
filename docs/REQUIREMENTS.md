# REQUIREMENTS — Bản Đặc Tả Yêu Cầu Hệ Thống

Tài liệu này định nghĩa phạm vi, yêu cầu chức năng, yêu cầu phi chức năng có số đo kiểm chứng, và tiêu chí nghiệm thu cho hệ thống **Real-time Anomaly Detection Pipeline**.

---

## 1. Bối cảnh & Mục tiêu

- **Bối cảnh:** Thị trường tiền mã hóa (Crypto) hoạt động 24/7 với biến động giá và thanh khoản rất nhanh. Các hiện tượng giật giá, xả hàng (dump), kéo giá (pump) hoặc nghẽn thanh khoản diễn ra trong tích tắc.
- **Mục tiêu:** Xây dựng một pipeline xử lý dữ liệu luồng thời gian thực từ sàn Binance (cặp BTCUSDT) chạy độc lập trên máy cá nhân:
  ```
  Binance WebSocket → Validate & Chuẩn hóa → SQLite (Idempotent) → Dựng Nến (5s) → Z-score Detector → Lưu Alert → REST API
  ```

---

## 2. User Stories (Câu chuyện người dùng)

- **US-1 (Nhà phân tích rủi ro / Trader):**
  > *Là một* nhà phân tích thị trường,
  > *Tôi muốn* nhận được cảnh báo tức thì kèm điểm bất thường (|z-score| >= 3.0) khi giá hoặc khối lượng BTCUSDT lệch chuẩn bất thường trong 10 phút gần nhất,
  > *Để* kịp thời phát hiện các đợt giật giá bất thường và quản trị rủi ro giao dịch.

- **US-2 (Kỹ sư dữ liệu - Data Engineer):**
  > *Là một* kỹ sư dữ liệu,
  > *Tôi muốn* hệ thống tự động kết nối lại khi rớt mạng và nạp dữ liệu một cách bất biến (Idempotent),
  > *Để* dữ liệu không bị thất thoát và không bao giờ bị nhân đôi (duplicate) khi chạy lại.

- **US-3 (Kỹ sư chất lượng dữ liệu - Data Quality Engineer):**
  > *Là một* kỹ sư đảm bảo chất lượng dữ liệu,
  > *Tôi muốn* mọi payload bị lỗi (giá âm, thiếu trường, timestamp trong tương lai) bị cách ly vào bảng `rejected_events` kèm lý do,
  > *Để* không làm sai lệch các chỉ số thống kê và dễ dàng truy vết nguồn gốc lỗi.

- **US-4 (Lập trình viên tích hợp - Backend Developer):**
  > *Là một* lập trình viên ứng dụng,
  > *Tôi muốn* có REST API để truy vấn danh sách nến (OHLCV) và lịch sử cảnh báo theo khoảng thời gian,
  > *Để* tích hợp hiển thị dữ liệu trực tiếp lên dashboard phân tích.

---

## 3. Yêu cầu chức năng (Functional Requirements - FR)

- **FR-1 (Ingestion thời gian thực & Replay):**
  - Kết nối trực tiếp vào Binance WebSocket Stream (`wss://stream.binance.com:9443/ws/btcusdt@trade`).
  - Hỗ trợ nguồn phát lại (`ReplaySource`) từ file JSON mẫu để kiểm thử offline mà không phụ thuộc vào internet.
- **FR-2 (Kiểm tra và chuẩn hóa - Validation):**
  - Kiểm tra tính toàn vẹn của dữ liệu: `symbol` hợp lệ, `price > 0`, `quantity > 0`, thời điểm khớp lệnh `trade_time_ms` không vượt quá thời gian hiện tại.
  - Tách bản ghi không hợp lệ lưu vào bảng `rejected_events` kèm timestamp và nguyên nhân lỗi.
- **FR-3 (Lưu trữ bất biến - Idempotent Storage):**
  - Lưu các giao dịch hợp lệ vào bảng `trades` với khóa chính tự nhiên là `(symbol, trade_id)`.
  - Cơ chế ghi theo micro-batch (500 records hoặc mỗi 1 giây) bằng `INSERT OR IGNORE` để không bao giờ sinh lỗi khi nhận lại giao dịch cũ.
- **FR-4 (Dựng nến thời gian - Bar Aggregator):**
  - Gom các giao dịch riêng lẻ thành nến thời gian 5 giây (5-second bars) gồm: `open`, `high`, `low`, `close`, `volume`, `trade_count`.
  - Khóa chính của bảng `bars`: `(symbol, bar_seconds, bar_start_ms)`.
- **FR-5 (Phát hiện bất thường thống kê - Anomaly Detection):**
  - Duy trì cửa sổ trượt (sliding window) gồm 120 nến gần nhất (~10 phút), yêu cầu tối thiểu 30 nến để bắt đầu tính toán (warm-up).
  - Tính z-score trên tỷ suất sinh lời log (log return) và khối lượng giao dịch.
  - Nếu |z-score| >= 3.0, gắn nhãn là sự kiện bất thường.
- **FR-6 (Lưu vết cảnh báo - Alert Persistence):**
  - Ghi nhận sự kiện bất thường vào bảng `alerts` với ràng buộc `UNIQUE(symbol, detector, bar_end_ms)` để không bao giờ ghi trùng cảnh báo cho cùng một cây nến.

---

## 4. Yêu cầu phi chức năng có số đo (Non-Functional Requirements - NFR)

- **NFR-1 (Thông lượng - Throughput):**
  - Hệ thống xử lý và ghi nhận ổn định tối thiểu **500 giao dịch/giây** trên môi trường máy cá nhân.
- **NFR-2 (Độ trễ - Latency):**
  - Độ trễ xử lý từ lúc nhận message từ WebSocket đến khi lưu DB và tính toán bất thường có **p95 <= 50 ms**.
- **NFR-3 (Độ tin cậy & Tự phục hồi - Resilience):**
  - Tự động kết nối lại WebSocket khi bị đứt mạng với cơ chế Exponential Backoff (thử lại sau 1s, 2s, 4s... tối đa 30s).
- **NFR-4 (Tính bất biến - Idempotency):**
  - Khi phát lại cùng một tập dữ liệu 10.000 giao dịch 2 lần liên tiếp, số lượng dòng mới phát sinh trong bảng `trades` và bảng `alerts` ở lần 2 là **chính xác bằng 0**.
- **NFR-5 (Tài nguyên - Resource Footprint):**
  - Bộ nhớ RAM tiêu thụ của pipeline không vượt quá **300 MB**.
  - CPU trung bình sử dụng dưới **15%** trên CPU 4 nhân máy cá nhân.

---

## 5. Ngoài phạm vi (Out of Scope)

Trong giai đoạn Sprint đến 30/10/2026, các hạng mục sau **không thực hiện**:
1. Đặt lệnh giao dịch thật hoặc bot tự động mua/bán (Automated Trading Execution).
2. Triển khai hạ tầng phân tán phức tạp (Kafka, Spark, Flink, ClickHouse, Docker Compose).
3. Hệ thống xác thực người dùng (User Authentication, JWT, OAuth2).
4. Phân tích mô hình học sâu (Deep Learning / LSTM / Transformers).

---

## 6. Câu hỏi còn mở (Open Questions)

1. **Xử lý giao dịch đến trễ (Out-of-order events):** Độ trễ mạng có thể làm một vài giao dịch thuộc cây nến trước đến sau khi nến đã đóng. Thời gian chấp nhận độ trễ (grace period) là bao nhiêu giây? *(Tạm quy ước: 2 giây)*.
2. **Khởi động nguội (Cold start):** Khi mới bật hệ thống lên chưa đủ 30 nến thì detector sẽ làm gì? *(Tạm quy ước: Tích lũy dữ liệu vào cửa sổ, chưa phát hiện cảnh báo cho đến khi đủ 30 nến)*.
