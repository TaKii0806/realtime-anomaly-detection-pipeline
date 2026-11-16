from pipeline.api.schemas import StatsResponse
from pipeline.api.schemas import AlertResponse
from pipeline.api.schemas import BarResponse
from pipeline.api.schemas import TradeResponse
from pipeline.api.schemas import HealthResponse
from fastapi import HTTPException
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Depends
from collections.abc import Iterator
import psycopg
import os
load_dotenv()


app = FastAPI(
    title="Crypto Anomaly Detection Pipeline API",
    version='0.1.0',
    description='REST API for inspecting ingested trades, 1-minutes OHLCV bars, Z-Score Alerts, and latency metrics'
)

def get_db() -> Iterator[psycopg.Connection]:
    DB_CONN = os.getenv('DATABASE_URL')
    if not DB_CONN:
        raise HTTPException(status_code=500, detail='DATABASE_URL is not configured')
    with psycopg.connect(DB_CONN) as conn:
        yield conn

@app.get("/health", response_model=HealthResponse)
def get_health(conn:psycopg.Connection=Depends(get_db)) -> HealthResponse:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return HealthResponse(status="ok", database="connected")
    except psycopg.Error as e:
        raise HTTPException(status_code=503,detail=f'DB Error: {e}')

@app.get("/trades/latest", response_model=list[TradeResponse])
def get_latest_trades(
    conn:psycopg.Connection=Depends(get_db), 
    limit:int=Query(default=20, ge=1, le=200),
    symbol:str="BTCUSDT") -> list[TradeResponse]:
    with conn.cursor() as cur:
        sql = """
        SELECT symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker FROM raw.trades
        WHERE symbol = %s
        ORDER BY trade_time_ms DESC, trade_id DESC
        LIMIT %s;
        """
        cur.execute(sql, (symbol, limit))
        row = cur.fetchall()
        if not row:
            return []
        return [TradeResponse(symbol=r[0], trade_id= r[1], price= r[2], quantity= r[3], trade_time_ms= r[4], is_buyer_maker= r[5]) for r in row]

@app.get("/bars", response_model=list[BarResponse])
def get_bars(
    conn:psycopg.Connection=Depends(get_db),
    limit:int = Query(default=50, ge=1, le=500),
    bar_seconds:int=Query(default=60, ge=1),
    symbol:str="BTCUSDT"
) -> list[BarResponse]:
    with conn.cursor() as cur:
        sql = """
        SELECT symbol, bar_seconds, bar_start_ms, open, high, low, close, volume, trade_count
        FROM ops.bars
        WHERE symbol = %s AND bar_seconds = %s
        ORDER BY bar_start_ms DESC
        LIMIT %s;
        """
        cur.execute(sql, (symbol, bar_seconds, limit))
        row = cur.fetchall()
        if not row:
            return []
        return[BarResponse(symbol=r[0], bar_seconds=r[1], bar_start_ms=r[2], open=r[3], high=r[4], low=r[5], close=r[6], volume=r[7], trade_count=r[8])
        for r in row]

@app.get("/alerts", response_model=list[AlertResponse])
def get_alerts(
    conn:psycopg.Connection=Depends(get_db),
    symbol:str="BTCUSDT",
    limit:int=Query(default=20, ge=1, le=200)
) -> list[AlertResponse]:
    with conn.cursor() as cur:
        sql = """
        SELECT alert_id, symbol, detector, bar_start_ms, bar_end_ms, score, threshold, details_json, created_at_ms
        FROM ops.alerts
        WHERE symbol = %s
        ORDER BY bar_end_ms DESC
        LIMIT %s
        """
        cur.execute(sql, (symbol, limit))
        row = cur.fetchall()
        if not row:
            return []
        return [AlertResponse(alert_id=r[0], symbol=r[1], detector=r[2], bar_start_ms=r[3], bar_end_ms=r[4], score=r[5], threshold=r[6], details_json=r[7], created_at_ms=r[8])
        for r in row]

@app.get("/stats", response_model=StatsResponse)
def get_stats(
    conn:psycopg.Connection=Depends(get_db),
    symbol:str="BTCUSDT"
) -> StatsResponse:
    with conn.cursor() as cur:
        sql1= """
        SELECT COUNT(*),
        percentile_cont(0.5) WITHIN GROUP (ORDER BY (ingested_at_ms - trade_time_ms)) AS p50,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY (ingested_at_ms - trade_time_ms)) AS p95
        FROM raw.trades WHERE symbol = %s;"""
        sql2 = "SELECT COUNT(*) FROM raw.rejected_events"
        cur.execute(sql1, (symbol,))
        ans = cur.fetchone()
        if not ans:
            total_trades, p50_latency_ms, p95_latency_ms = (0, None, None)
        else:
            total_trades, p50_latency_ms, p95_latency_ms = ans
        cur.execute(sql2)
        ans = cur.fetchone()
        if not ans:
            total_rejected = 0
        else:
            total_rejected = ans[0]
        error_rate = 0.0
        if total_trades != 0:
            error_rate = total_rejected / (total_trades + total_rejected)
        return StatsResponse(symbol=symbol, total_trades=total_trades, total_rejected=total_rejected, error_rate=error_rate, p50_latency_ms=p50_latency_ms, p95_latency_ms=p95_latency_ms)
