from pipeline.models import Trade
import math

class ValidationError(Exception):
    pass

def parse_trade(raw:dict, *, expected_symbol:str="BTCUSDT", now_ms:int | None = None) -> Trade:
    if not isinstance(raw, dict):
        raise ValidationError("Payload must be a dict.")
    for x in ['e', 's', 't', 'p', 'q', 'T', 'm']:
        if x not in raw:
            raise ValidationError("Missing required fields")
    if raw['e'] != 'trade':
        raise ValidationError("Event type must be trade")
    if raw['s'] != expected_symbol:
        raise ValidationError("Symbol mismatch")
    try:
        trade_id = int(raw['t'])
        trade_time_ms = int(raw["T"])
        is_buyer_maker = bool(raw["m"])
        price = float(raw['p'])
        quantity = float(raw['q'])
    except (ValueError, TypeError) as err:
        raise ValidationError(f'Invalid number format: {err}')
    if math.isnan(price) or math.isnan(quantity) or math.isinf(quantity) or math.isinf(price):
        raise ValidationError("Price and quantity mustn't be NaN or Inf")
    if price <= 0 or quantity <= 0:
        raise ValidationError("Price and quantity must be positive")
    if now_ms is not None:
        if trade_time_ms > now_ms + 5000:  # Lệch tương lai quá 5 giây
            raise ValidationError("Trade time is in the future")

    return Trade(expected_symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker)