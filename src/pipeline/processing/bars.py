#OHLCV Time Bar Aggregator for trade streams.

from dataclasses import dataclass
from typing import Sequence
from pipeline.models import Trade

@dataclass(frozen=True)
class Bar:
    symbol: str
    bar_seconds: int
    bar_start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int

def build_bars(trades: Sequence[Trade], bar_seconds:int=60) -> list[Bar]:
    if not trades:
        return []

    interval_ms = bar_seconds * 1000
    bars: list[Bar] = []

    trade_groups = dict()
    for trade in trades:
        bar_start_ms = (trade.trade_time_ms // interval_ms) * interval_ms
        if (trade.symbol, bar_start_ms) not in trade_groups:
            trade_groups[trade.symbol, bar_start_ms] = []
        trade_groups[(trade.symbol, bar_start_ms)].append(trade)
    for key, value in trade_groups.items():
        open_price = value[0].price
        close_price = value[-1].price
        high = max(x.price for x in value)
        low = min(x.price for x in value)
        volume = sum([x.quantity for x in value])
        trade_count = len(value)
        bar = Bar(key[0], bar_seconds, key[1], open_price, high, low, close_price, volume, trade_count)
        bars.append(bar)
    return bars