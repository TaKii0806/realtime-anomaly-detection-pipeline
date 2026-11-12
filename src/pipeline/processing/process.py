"""
Consume raw trades, aggregates closed OHLCV bars, execute anomaly detection
"""

import json
import logging
import time
from typing import Sequence
import psycopg
from dotenv import load_dotenv
import os
from collections import deque

from pipeline.models import Trade
from pipeline.processing.bars import Bar, build_bars
from pipeline.processing.detectors import RollingZScoreDetector, Alert

logger = logging.getLogger("pipeline.process")

def get_checkpoint(cur: psycopg.Cursor, name:str) -> int:
    #Retrieve the last processed millisecond timestamp for a given workflow
    cur.execute("SELECT value FROM ops.checkpoints WHERE name = %s;", (name,))
    row = cur.fetchone()
    return int(row[0]) if row else 0

def save_checkpoint(cur: psycopg.Cursor, name:str, value:int, now_ms:int) -> None:
    #Upsert the operational checkpoint
    sql = """
    INSERT INTO ops.checkpoints (name, value, updated_at_ms)
    VALUES (%s, %s, %s)
    ON CONFLICT (name) DO UPDATE
    SET value = EXCLUDED.value, updated_at_ms = EXCLUDED.updated_at_ms;
    """
    cur.execute(sql, (name, value, now_ms))

def fetch_trades_range(cur: psycopg.Cursor, symbol:str, start_ms:int, end_ms:int) -> list[Trade]:
    #Fetch all trades withing [start_ms, end_ms)
    sql = """
    SELECT symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker
    FROM raw.trades
    WHERE symbol = %s AND trade_time_ms >= %s AND trade_time_ms < %s
    ORDER BY trade_time_ms ASC, trade_id ASC;
    """
    cur.execute(sql, (symbol, start_ms, end_ms))
    rows = cur.fetchall()
    return [
        Trade(symbol=r[0], trade_id=r[1], price=float(r[2]), quantity=float(r[3]), trade_time_ms=r[4], is_buyer_maker=r[5])
        for r in rows
    ]
    
def save_bars(cur: psycopg.Cursor, bars: Sequence[Bar]) -> None:
    #Save aggregated bars into ops.bars
    if not bars:
        return
    sql = """
    INSERT INTO ops.bars (symbol, bar_seconds, bar_start_ms, open, high, low, close, volume, trade_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, bar_seconds, bar_start_ms) DO NOTHING;
    """
    params = [(b.symbol, b.bar_seconds, b.bar_start_ms, b.open, b.high, b.low, b.close, b.volume, b.trade_count) for b in bars]
    cur.executemany(sql, params)

def save_alerts(cur: psycopg.Cursor, alerts: Sequence[Alert], detector_name:str = "z_score", bar_seconds: int = 60) -> None:
    #Save triggered alerts into ops.alerts
    if not alerts:
        return
    sql = """
    INSERT INTO ops.alerts (symbol, detector, bar_start_ms, bar_end_ms, score, threshold, details_json, created_at_ms)
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (symbol, detector, bar_end_ms) DO NOTHING;
    """
    params = []
    for a in alerts:
        bar_end_ms = a.bar_start_ms + bar_seconds * 1000
        details = json.dumps({"alert_type": a.alert_type, "severity": a.severity})
        params.append((a.symbol, detector_name, a.bar_start_ms, bar_end_ms, a.metric_value, a.threshold, details, a.created_at_ms))
    cur.executemany(sql, params)

def run_processing_cycle(
    conn_info:str,
    detector: RollingZScoreDetector,
    symbol: str = "BTCUSDT",
    bar_seconds: int=60,
    grace_period_ms: int = 5000,
    checkpoint_name: str="bars_1m_worker",
    now_ms: int | None = None
) -> dict[str, int]:
    """
    Execute a single atomic processing cycle:
    1. Read last checkpoint.
    2. Determine safe closed watermark: current_time grace_period.
    3. If there are closed intervals, fetch trades, aggregate bars, and run detector.
    4. Persist bars, alerts, and upd checkpoint inside a single transaction
    """

    interval_ms = bar_seconds * 1000
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    watermark_ms = now_ms - grace_period_ms

    with psycopg.connect(conn_info) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                start_ms = get_checkpoint(cur, checkpoint_name)
                if start_ms == 0:
                    sql = "SELECT MIN(trade_time_ms) FROM raw.trades WHERE symbol = %s"
                    cur.execute(sql, (symbol, ))
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        logger.info("No trades available for %s.", symbol)
                        return {"bars":0, "alerts":0}
                    start_ms = int(row[0] // interval_ms) * interval_ms
                end_ms = (watermark_ms // interval_ms) * interval_ms
                if end_ms <= start_ms:
                    return {"bars":0, "alerts":0}
                trades = fetch_trades_range(cur, symbol, start_ms, end_ms)
                bars = build_bars(trades, bar_seconds)
                bars = [b for b in bars if (b.bar_start_ms + interval_ms <= end_ms)]
                
                alerts = []
                for b in bars:
                    alert = detector.process(b)
                    if alert:
                        alerts.append(alert)
                
                save_bars(cur, bars)
                save_alerts(cur, alerts, detector_name= 'z_score', bar_seconds=bar_seconds)
                save_checkpoint(cur, checkpoint_name, end_ms, now_ms)
                
                return {"bars": len(bars), "alerts": len(alerts), "new_checkpoint": end_ms}

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    load_dotenv()
    DB_CONN = os.getenv("DATABASE_URL")
    if not DB_CONN:
        raise ValueError ("DATABASE_URL is not set. Please define it in your .env file or system environment.")
    detector = RollingZScoreDetector()
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            sql = """SELECT close FROM ops.bars 
            WHERE symbol = %s AND bar_seconds = %s
            ORDER BY bar_start_ms DESC
            LIMIT 30
            """
            cur.execute(sql, ("BTCUSDT", 60))
            rows = cur.fetchall()
    detector.windows["BTCUSDT"] = deque(
        (float(r[0]) for r in reversed(rows)),
        maxlen=detector.window_size,
    )
    result = run_processing_cycle(DB_CONN, detector)
    logger.info("Processing cycle completed: %s", result)