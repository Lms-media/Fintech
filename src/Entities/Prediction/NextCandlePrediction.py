from Interfaces import ICandle, ICandleSeries
from .Interfaces import INextCandlePrediction
from .APrediction import APrediction

class NextCandlePrediction(APrediction, INextCandlePrediction):
    _nextCandle: ICandle

    def __init__(self, timestamp: int, candleSeries: ICandleSeries, confidence: float, nextCandle: ICandle):
        super().__init__(timestamp, candleSeries, confidence)
        self._nextCandle = nextCandle

    def getNextCandle(self) -> ICandle:
        return self._nextCandle

    def __str__(self) -> str:
        return f"{super().__str__()} Next candle: {self.getNextCandle()}"
