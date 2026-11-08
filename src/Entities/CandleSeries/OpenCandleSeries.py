"""Detached information available at the opening of a candle."""
from dataclasses import dataclass
from math import isfinite
import uuid

from Interfaces import IReadonlyCandleSeries, IAssetPair, IntervalType
from ValueObjects import Candle


@dataclass(frozen=True)
class CandleOpen:
    assetPair: IAssetPair
    timestamp: int
    interval: IntervalType
    price: float

    def __post_init__(self):
        if self.timestamp < 0 or not isfinite(self.price) or self.price <= 0:
            raise ValueError("Invalid candle opening")


class OpenCandleSeries(IReadonlyCandleSeries):
    """Closed history only; current H/L/C/volume are deliberately absent."""

    def __init__(self, history: IReadonlyCandleSeries, current: CandleOpen):
        self._id = str(uuid.uuid4())
        self._current = current
        self._candles = tuple(
            self._copy(history.getByIndex(i)) for i in range(history.getCount())
        )
        previous_end = -1
        for candle in self._candles:
            if candle.getAssetPair() != current.assetPair or candle.getInterval() != current.interval:
                raise ValueError("History and current candle must have the same asset and interval")
            if candle.getOpenTimestamp() < previous_end:
                raise ValueError("History must be chronological and non-overlapping")
            previous_end = candle.getOpenTimestamp() + candle.getInterval().value
            if previous_end > current.timestamp:
                raise ValueError("History contains a candle not closed at decision time")

    @staticmethod
    def _copy(candle):
        if candle is None:
            raise ValueError("Missing history candle")
        prices = [candle.getOpenPrice(), candle.getClosePrice(), candle.getHighPrice(), candle.getLowPrice()]
        if any(not isfinite(p) or p <= 0 for p in prices) or not isfinite(candle.getVolume()):
            raise ValueError("History requires finite positive prices and finite volume")
        return Candle(candle.getAssetPair(), candle.getOpenTimestamp(), candle.getInterval(),
                      *prices, candle.getVolume())

    @classmethod
    def atIndex(cls, series, index, lookback):
        from .TrimmedCandleSeries import TrimmedCandleSeries
        if lookback < 1 or index < lookback or index >= series.getCount():
            raise ValueError("Not enough history or invalid prediction index")
        candle = series.getByIndex(index)
        # Never copy the target's H/L/C or volume into the prediction input.
        current = CandleOpen(series.getAssetPair(), candle.getOpenTimestamp(),
                             candle.getInterval(), candle.getOpenPrice())
        return cls(TrimmedCandleSeries(series, index - lookback, index), current)

    def getCurrentOpen(self):
        return self._current

    def __str__(self):
        return f"OpenCandleSeries(closed={self.getCount()}, current={self._current})"

    def getId(self):
        return self._id

    def getCount(self):
        return len(self._candles)

    def getAssetPair(self):
        return self._current.assetPair

    def getByIndex(self, index):
        return self._candles[index] if 0 <= index < len(self._candles) else None

    def getIndexOf(self, candle):
        return next((i for i, item in enumerate(self._candles) if item == candle), -1)

    def getByTimestamp(self, timestamp):
        return next((c for c in self._candles
                     if c.getOpenTimestamp() <= timestamp < c.getOpenTimestamp() + c.getInterval().value), None)

