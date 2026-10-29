# PostgreSQL repository for persisting and querying market trades.
import asyncio
import time
from typing import Sequence, Any
import psycopg
from pipeline.models import Trade

class PostgresTradeRepository:
    #Repository handling database operations for raw trades in PostgreSQL

    def __init__ (self, conn_info:str) -> None:
        self.conn_info = conn_info

    def insert_many(self, trades: Sequence[Trade]) -> int:
        #Insert batch of trades into raw.trades
        if not trades:
            return 0
        now_ms = int(time.time() * 1000)

        sql = """
        INSERT INTO raw.trades(symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trade_id) DO NOTHING;
        """
        params = [
            (trade.symbol, trade.trade_id, trade.price, trade.quantity, trade.trade_time_ms, trade.is_buyer_maker, now_ms)
            for trade in trades
        ]

        with psycopg.connect(self.conn_info) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
                return cur.rowcount

    def count(self, symbol:str) -> int:
        #Return total count of trades for the given symbol
        with psycopg.connect(self.conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw.trades WHERE symbol = %s", (symbol,))
                row = cur.fetchone()
                return row[0] if row else 0
    
    def fetch_since(self, symbol:str, since_ms:int, limit:int=1000) -> list[Trade]:
        with psycopg.connect(self.conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker
                FROM raw.trades
                WHERE symbol = %s and trade_time_ms >= %s
                LIMIT %s
                """, (symbol, since_ms, limit))
                row = cur.fetchall()
                return [Trade(
                symbol=r[0],
                trade_id = r[1], 
                price = r[2], 
                quantity = r[3], 
                trade_time_ms=r[4], 
                is_buyer_maker=r[5]
                ) for r in row]

class PostgresSink:
    #Storage sink that persist micro-batches into PostgreSQL
    def __init__(self, repo: PostgresTradeRepository) -> None:
        self.repo = repo

    async def write(self, items: Sequence[Any]) -> None:
        if not items:
            return
        await asyncio.to_thread(self.repo.insert_many, items)