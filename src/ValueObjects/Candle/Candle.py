from __future__ import annotations
from Interfaces import ICandle, IAssetPair, IntervalType

class Candle(ICandle):
    _assetPair: IAssetPair
    _openTimestamp: int
    _interval: IntervalType
    _openPrice: float
    _closePrice: float
    _highPrice: float
    _lowPrice: float
    _vloume: float

    def __init__(self, assetPair: IAssetPair, openTimestamp: int, interval: IntervalType, openPrice: float, closePrice: float, highPrice: float, lowPrice: float, volume: float):
        if lowPrice > highPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'highPrice', but 'lowPrice' is {lowPrice} and 'highPrice' is {highPrice}")

        if openTimestamp < 0:
            raise ValueError(f"'openTimestamp' must be greater than or equal to zero, but 'openTimestamp' is {openTimestamp}")

        if openPrice < 0:
            raise ValueError(f"'openPrice' must be greater than or equal to zero, but 'openPrice' is {openPrice}")

        if closePrice < 0:
            raise ValueError(f"'closePrice' must be greater than or equal to zero, but 'closePrice' is {closePrice}")

        if highPrice < 0:
            raise ValueError(f"'highPrice' must be greater than or equal to zero, but 'highPrice' is {highPrice}")

        if lowPrice < 0:
            raise ValueError(f"'lowPrice' must be greater than or equal to zero, but 'lowPrice' is {lowPrice}")

        if volume < 0:
            raise ValueError(f"'volume' must be greater than or equal to zero, but 'volume' is {volume}")

        if highPrice < openPrice:
            raise ValueError(f"'highPrice' must be greater than or equal to 'openPrice', but 'highPrice' is {highPrice} and 'openPrice' is {openPrice}")

        if highPrice < closePrice:
            raise ValueError(f"'highPrice' must be greater than or equal to 'closePrice', but 'highPrice' is {highPrice} and 'closePrice' is {closePrice}")

        if lowPrice > openPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'openPrice', but 'lowPrice' is {lowPrice} and 'openPrice' is {openPrice}")

        if lowPrice > closePrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'closePrice', but 'lowPrice' is {lowPrice} and 'closePrice' is {closePrice}")

        self._assetPair = assetPair
        self._openTimestamp = openTimestamp
        self._interval = interval
        self._openPrice = openPrice
        self._closePrice = closePrice
        self._highPrice = highPrice
        self._lowPrice = lowPrice
        self._volume = volume

    def getAssetPair(self):
        return self._assetPair

    def getOpenTimestamp(self):
        return self._openTimestamp

    def getInterval(self):
        return self._interval

    def getOpenPrice(self):
        return self._openPrice

    def getClosePrice(self):
        return self._closePrice

    def getHighPrice(self):
        return self._highPrice

    def getLowPrice(self):
        return self._lowPrice

    def getVolume(self):
        return self._volume

    def withAssetPair(self, assetPair):
        return Candle(assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withOpenTimestamp(self, openTimestamp):
        if openTimestamp < 0:
            raise ValueError(f"'openTimestamp' must be greater than or equal to zero, but 'openTimestamp' is {openTimestamp}")

        return Candle(self._assetPair, openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withInterval(self, interval):
        return Candle(self._assetPair, self._openTimestamp, interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withOpenPrice(self, openPrice):
        if openPrice < 0:
            raise ValueError(f"'openPrice' must be greater than or equal to zero, but 'openPrice' is {openPrice}")

        if self._highPrice < openPrice:
            raise ValueError(f"'highPrice' must be greater than or equal to 'openPrice', but 'highPrice' is {self._highPrice} and 'openPrice' is {openPrice}")

        if self._lowPrice > openPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'openPrice', but 'lowPrice' is {self._lowPrice} and 'openPrice' is {openPrice}")

        return Candle(self._assetPair, self._openTimestamp, self._interval, openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withClosePrice(self, closePrice):
        if closePrice < 0:
            raise ValueError(f"'closePrice' must be greater than or equal to zero, but 'closePrice' is {closePrice}")

        if self._highPrice < closePrice:
            raise ValueError(f"'highPrice' must be greater than or equal to 'closePrice', but 'highPrice' is {self._highPrice} and 'closePrice' is {closePrice}")

        if self._lowPrice > closePrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'closePrice', but 'lowPrice' is {self._lowPrice} and 'closePrice' is {closePrice}")

        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, closePrice, self._highPrice, self._lowPrice, self._volume)

    def withHighPrice(self, highPrice):
        if self._lowPrice > highPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'highPrice', but 'lowPrice' is {self._lowPrice} and 'highPrice' is {highPrice}")

        if highPrice < 0:
            raise ValueError(f"'highPrice' must be greater than or equal to zero, but 'highPrice' is {highPrice}")

        if highPrice < self._openPrice:
            raise ValueError(f"'highPrice' must be greater than or equal to 'openPrice', but 'highPrice' is {highPrice} and 'openPrice' is {self._openPrice}")

        if highPrice < self._closePrice:
            raise ValueError(f"'highPrice' must be greater than or equal to 'closePrice', but 'highPrice' is {highPrice} and 'closePrice' is {self._closePrice}")

        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, highPrice, self._lowPrice, self._volume)

    def withLowPrice(self, lowPrice):
        if lowPrice > self._highPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'highPrice', but 'lowPrice' is {lowPrice} and 'highPrice' is {self._highPrice}")

        if lowPrice < 0:
            raise ValueError(f"'lowPrice' must be greater than or equal to zero, but 'lowPrice' is {lowPrice}")

        if lowPrice > self._openPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'openPrice', but 'lowPrice' is {lowPrice} and 'openPrice' is {self._openPrice}")

        if lowPrice > self._closePrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'closePrice', but 'lowPrice' is {lowPrice} and 'closePrice' is {self._closePrice}")

        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, lowPrice, self._volume)

    def withVolume(self, volume):
        if volume < 0:
            raise ValueError(f"'volume' must be greater than or equal to zero, but 'volume' is {volume}")

        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, volume)

    def __eq__(self, other):
        if not isinstance(other, Candle):
            return False
        return (self._assetPair == other.getAssetPair() and
                self._openTimestamp == other.getOpenTimestamp() and
                self._interval == other.getInterval() and
                self._openPrice == other.getOpenPrice() and
                self._closePrice == other.getClosePrice() and
                self._highPrice == other.getHighPrice() and
                self._lowPrice == other.getLowPrice() and
                self._volume == other.getVolume())

    def __hash__(self):
        return hash((
            self._openTimestamp,
            self._interval,
            self._openPrice,
            self._closePrice,
            self._highPrice,
            self._lowPrice,
            self._volume
        ))

    def __copy__(self):
        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def __str__(self):
        return (f"📊 {{{self._assetPair}, timestamp={self._openTimestamp}, interval={self._interval.name}, O={self._openPrice}, C={self._closePrice}, H={self._highPrice}, L={self._lowPrice}, V={self._volume}}}")
