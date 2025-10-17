import abc
from datetime import datetime as dt
from src.data.Candle import Candle

class DataSource(abc.ABC):
    def __init__(self, interval: int, tickerCode: str):
        self.interval = interval
        self.candles: list[Candle] = []
        self.size = len(self.candles)
        self.tickerCode = tickerCode
    def getSlice(self, dateFrom: dt, dateTo: dt) -> list[Candle]:
        foundCandles = []
        for candle in self.candles:
            if dateFrom <= candle.datetime <= dateTo:
                foundCandles.append(candle)
                
        return foundCandles
    def nextCandle(self, candle: Candle):
        if candle in self.candles:
            return self.candles[self.candles.index(candle) + 1]
        return None