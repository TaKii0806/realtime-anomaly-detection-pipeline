from dataclasses import dataclass

@dataclass(frozen=True)
class Trade:
    symbol: str
    trade_id: int
    price: float
    quantity: float
    trade_time_ms: int
    is_buyer_maker: bool