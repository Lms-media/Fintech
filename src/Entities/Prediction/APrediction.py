from abc import ABC
from Interfaces import IPrediction, ICandleSeries

class APrediction(IPrediction, ABC):
    _timestamp: int
    _candleSeries: ICandleSeries
    _confidence: float

    def __init__(self, timestamp: int, candleSeries: ICandleSeries, confidence: float):
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        if confidence < 0 or confidence > 1:
            raise ValueError(f"'confidence' must be between 0 and 1, but 'confidence' is {confidence}")

        self._timestamp = timestamp
        self._candleSeries = candleSeries
        self._confidence = confidence

    def getTimestamp(self) -> int:
        return self._timestamp

    def getCandleSeries(self) -> ICandleSeries:
        return self._candleSeries

    def getConfidence(self) -> float:
        return self._confidence
