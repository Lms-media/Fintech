from Interfaces import ICandle, ICandleSeries
from Models.Entities import APrediction

class NextCandlePrediction(APrediction):
    _nextCandle: ICandle

    def __init__(self, timestamp: int, candleSeries: ICandleSeries, confidence: float, nextCandle: ICandle):
        super().__init__(timestamp, candleSeries, confidence)
        self._nextCandle = nextCandle

    def getNextCandle(self) -> ICandle:
        return self._nextCandle
