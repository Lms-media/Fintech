from src.Interfaces import ICandle, IAssetPair, IntervalType

class Candle(ICandle):

    def __init__(self, assetPair: IAssetPair, openTimestamp: int, interval: IntervalType, openPrice: float, closePrice: float, highPrice: float, lowPrice: float, volume: float):
        if lowPrice > highPrice:
            raise ValueError(f"'lowPrice' must be less than or equal to 'highPrice'")

        if openTimestamp < 0:
            raise ValueError(f"'openTimestamp' must be greater than or equal to zero")

        if openPrice < 0:
            raise ValueError(f"'openPrice' must be greater than or equal to zero")

        if closePrice < 0:
            raise ValueError(f"'closePrice' must be greater than or equal to zero")

        if highPrice < 0:
            raise ValueError(f"'highPrice' must be greater than or equal to zero")

        if lowPrice < 0:
            raise ValueError(f"'lowPrice' must be greater than or equal to zero")

        if volume < 0:
            raise ValueError(f"'volume' must be greater than or equal to zero")

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
        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withOpenTimestamp(self, openTimestamp):
        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withInterval(self, interval):
        return Candle(self._assetPair, self._openTimestamp, interval, self._openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withOpenPrice(self, openPrice):
        return Candle(self._assetPair, self._openTimestamp, self._interval, openPrice, self._closePrice, self._highPrice, self._lowPrice, self._volume)

    def withClosePrice(self, closePrice):
        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, closePrice, self._highPrice, self._lowPrice, self._volume)

    def withHighPrice(self, highPrice):
        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, highPrice, self._lowPrice, self._volume)

    def withLowPrice(self, lowPrice):
        return Candle(self._assetPair, self._openTimestamp, self._interval, self._openPrice, self._closePrice, self._highPrice, lowPrice, self._volume)

    def withVolume(self, volume):
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
