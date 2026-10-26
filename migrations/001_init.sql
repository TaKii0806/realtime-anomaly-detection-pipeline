-- migrations/001_init.sql
-- Khởi tạo cấu trúc cơ sở dữ liệu cho Real-time Anomaly Detection Pipeline

-- 1. Bảng lưu trữ giao dịch khớp lệnh hợp lệ (Idempotent theo symbol và trade_id)
CREATE TABLE IF NOT EXISTS trades (
    symbol          TEXT    NOT NULL,
    trade_id        INTEGER NOT NULL,            -- id do Binance cấp (khóa tự nhiên)
    price           REAL    NOT NULL CHECK (price > 0),
    quantity        REAL    NOT NULL CHECK (quantity > 0),
    trade_time_ms   INTEGER NOT NULL,            -- thời điểm khớp lệnh, UTC epoch ms
    is_buyer_maker  INTEGER NOT NULL CHECK (is_buyer_maker IN (0, 1)),
    ingested_at_ms  INTEGER NOT NULL,            -- thời điểm hệ thống ghi nhận
    PRIMARY KEY (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, trade_time_ms);

-- 2. Bảng cách ly dữ liệu lỗi (Dead-letter Queue)
CREATE TABLE IF NOT EXISTS rejected_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at_ms  INTEGER NOT NULL,
    reason          TEXT    NOT NULL,
    raw_payload     TEXT    NOT NULL
);

-- 3. Bảng nến tổng hợp theo thời gian (Time Bars)
CREATE TABLE IF NOT EXISTS bars (
    symbol          TEXT    NOT NULL,
    bar_seconds     INTEGER NOT NULL,
    bar_start_ms    INTEGER NOT NULL,
    open            REAL    NOT NULL,
    high            REAL    NOT NULL,
    low             REAL    NOT NULL,
    close           REAL    NOT NULL,
    volume          REAL    NOT NULL,
    trade_count     INTEGER NOT NULL,
    PRIMARY KEY (symbol, bar_seconds, bar_start_ms)
);

-- 4. Bảng lưu vết các sự kiện bất thường (Alerts)
CREATE TABLE IF NOT EXISTS alerts (
    alert_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT    NOT NULL,
    detector        TEXT    NOT NULL,            -- ví dụ: zscore_return, zscore_volume, isolation_forest
    bar_start_ms    INTEGER NOT NULL,
    bar_end_ms      INTEGER NOT NULL,
    score           REAL    NOT NULL,
    threshold       REAL    NOT NULL,
    details_json    TEXT    NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    UNIQUE (symbol, detector, bar_end_ms)
);

CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts (bar_end_ms);

-- 5. Bảng checkpoint tiến trình
CREATE TABLE IF NOT EXISTS checkpoints (
    name            TEXT    PRIMARY KEY,
    value           INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);
