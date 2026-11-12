from collections import deque
from dataclasses import dataclass
from typing import Protocol
from pipeline.processing.bars import Bar
import numpy as np
import time

@dataclass(frozen=True)
class Alert:
    symbol: str
    alert_type: str
    severity: str
    metric_value: float
    threshold: float
    bar_start_ms: int
    created_at_ms: int

class Detector(Protocol):
    def process(self, bar: Bar) -> Alert | None:
        ...

class RollingZScoreDetector:
    def __init__(
        self,
        window_size: int = 30,
        threshold: float = 3.0,
        min_periods: int = 10,
        min_pct_change: float = 0.005,
    ) -> None:
        self.window_size = window_size
        self.threshold = threshold
        self.min_periods = min_periods
        self.min_pct_change = min_pct_change
        self.windows: dict[str, deque[float]] = {}
    def process(self, bar: Bar) -> Alert | None:
        if bar.symbol not in self.windows:
            self.windows[bar.symbol] = deque(maxlen=self.window_size)

        history = self.windows[bar.symbol]
        alert: Alert | None = None

        if (len(history) >= self.min_periods):
            mean_price = np.mean(history, dtype=float)
            std_price = np.std(history, dtype=float)
            std_price = max(std_price, 1e-6)
            z_score = (bar.close - mean_price) / std_price
            pct_change = abs(bar.close - mean_price) / mean_price
            if abs(z_score) >= self.threshold and pct_change >= self.min_pct_change:
                alert_type = "PRICE_SPIKE" if z_score > 0 else "PRICE_PLUNGE"
                created_ms = int(time.time() * 1000)
                alert = Alert(bar.symbol, alert_type, "WARNING", z_score, self.threshold, bar.bar_start_ms, created_ms)
        history.append(bar.close)
        return alert

