"""
Database Performance Benchmarking Suite.
Measures ingestion throughput (Single vs Batch) and query performance (Seq Scan vs Index Scan).
"""

import os
import time
import psycopg
from dotenv import load_dotenv

load_dotenv()
DB_CONN = os.getenv(
    "DATABASE_URL",
    "host=localhost port=5432 dbname=crypto_dw user=crypto_user password=crypto_secret_password_2026",
)


def benchmark_insert(num_records: int = 5000) -> None:
    """Benchmark Single-row Insert vs Micro-batch executemany."""
    print(f"\n=======================================================")
    print(f"1. BENCHMARKING INGESTION THROUGHPUT ({num_records:,} records)")
    print(f"=======================================================")

    now_ms = int(time.time() * 1000)
    sql_insert = """
    INSERT INTO raw.trades (symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, trade_id) DO NOTHING;
    """

    # --- Bài đo A: Single Insert (từng dòng đơn lẻ trên 200 dòng mẫu) ---
    sample_single = 200
    single_trades = [
        ("BENCH_SINGLE", 8000000 + i, 85000.0 + (i % 50), 0.05, now_ms + i, False, now_ms)
        for i in range(sample_single)
    ]
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            start_t = time.perf_counter()
            for trade in single_trades:
                cur.execute(sql_insert, trade)
                conn.commit()  # Mỗi lệnh 1 transaction riêng
            duration_single = time.perf_counter() - start_t
            rate_single = sample_single / duration_single
            print(f"[*] Cách 1 (Single Insert + autocommit): {sample_single} rows in {duration_single:.3f}s -> {rate_single:.1f} rows/s")

    # --- Bài đo B: Micro-batch executemany (5,000 dòng gom theo mẻ 500) ---
    batch_size = 500
    batch_trades = [
        ("BENCH_BATCH", 9000000 + i, 85000.0 + (i % 50), 0.05, now_ms + i, False, now_ms)
        for i in range(num_records)
    ]
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            start_t = time.perf_counter()
            for i in range(0, num_records, batch_size):
                chunk = batch_trades[i : i + batch_size]
                cur.executemany(sql_insert, chunk)
            conn.commit()
            duration_batch = time.perf_counter() - start_t
            rate_batch = num_records / duration_batch
            speedup = rate_batch / rate_single
            print(f"[*] Cách 2 (Micro-batch executemany):   {num_records:,} rows in {duration_batch:.3f}s -> {rate_batch:.1f} rows/s")
            print(f"[===>] KET LUAN: Micro-batch tang toc GAP {speedup:.1f} LAN so voi Single Insert!\n")

    # Dọn dẹp dữ liệu benchmark
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.trades WHERE symbol IN ('BENCH_SINGLE', 'BENCH_BATCH');")
            conn.commit()


def benchmark_query_with_explain() -> None:
    """Benchmark OHLCV query with and without Index using EXPLAIN (ANALYZE, BUFFERS)."""
    print(f"=======================================================")
    print(f"2. BENCHMARKING QUERY TUNING (EXPLAIN ANALYZE)")
    print(f"=======================================================")

    # Bơm 20,000 dòng dữ liệu vào bảng để thấy rõ sự khác biệt giữa Seq Scan và Index Scan
    now_ms = int(time.time() * 1000)
    data_points = 20000
    synthetic_trades = [
        ("BTC_BENCH", 5000000 + i, 80000.0 + (i % 200), 0.01, now_ms + (i * 100), False, now_ms)
        for i in range(data_points)
    ]
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.trades (symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, trade_id) DO NOTHING;
                """,
                synthetic_trades,
            )
            conn.commit()

    query = """
    EXPLAIN (ANALYZE, BUFFERS)
    SELECT 
        symbol,
        MIN(price) AS low,
        MAX(price) AS high,
        AVG(price) AS avg_price,
        COUNT(*) AS total_trades
    FROM raw.trades
    WHERE symbol = %s AND trade_time_ms BETWEEN %s AND %s
    GROUP BY symbol;
    """
    params = ("BTC_BENCH", now_ms, now_ms + 100000)

    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            # Lần 1: Ép tắt Index Scan -> Ép dùng Sequential Scan
            cur.execute("SET enable_indexscan = off; SET enable_bitmapscan = off;")
            cur.execute(query, params)
            plan_seq = "\n".join([row[0] for row in cur.fetchall()])
            print("\n[PLAN 1: SEQUENTIAL SCAN (TAT INDEX)]:\n" + plan_seq)

            # Lần 2: Bật lại Index Scan
            cur.execute("SET enable_indexscan = on; SET enable_bitmapscan = on;")
            cur.execute(query, params)
            plan_idx = "\n".join([row[0] for row in cur.fetchall()])
            print("\n[PLAN 2: INDEX SCAN (CO B-TREE INDEX)]:\n" + plan_idx)

            # Dọn dẹp
            cur.execute("DELETE FROM raw.trades WHERE symbol = 'BTC_BENCH';")
            conn.commit()


if __name__ == "__main__":
    benchmark_insert(num_records=5000)
    benchmark_query_with_explain()

