"""Pydantic response schemas for the Crypto Anomaly Detection API."""

from typing import Any
from pydantic import BaseModel

class HealthResponse(BaseModel):
    status: str
    database: str

class TradeResponse(BaseModel):
    symbol: str
    trade_id: int
    price: float
    quantity: float
    trade_time_ms: int
    is_buyer_maker: bool

class BarResponse(BaseModel):
    symbol: str
    bar_seconds: int
    bar_start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int

class AlertResponse(BaseModel):
    alert_id: int
    symbol: str
    detector: str
    bar_start_ms: int
    bar_end_ms: int
    score: float
    threshold: float
    details_json: dict[str, Any]
    created_at_ms: int

class StatsResponse(BaseModel):
    symbol: str
    total_trades: int
    total_rejected: int
    error_rate: float
    p50_latency_ms: float | None
    p95_latency_ms: float | None