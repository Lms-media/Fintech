from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class Candle:
    open: float
    close: float
    high: float
    low: float
    volume: float
    datetime: datetime
    interval: int
