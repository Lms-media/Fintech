import uuid
from Interfaces import IPredictionMeta, ICandleSeries

class PredictionMeta(IPredictionMeta):
    _id: str
    _timestamp: int
    _candleSeries: ICandleSeries
    _confidence: float

    def __init__(self, timestamp: int, candleSeries: ICandleSeries, confidence: float):
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        if confidence < 0 or confidence > 1:
            raise ValueError(f"'confidence' must be between 0 and 1, but 'confidence' is {confidence}")

        self._id = uuid.uuid4()
        self._timestamp = timestamp
        self._candleSeries = candleSeries
        self._confidence = confidence

    def getId(self) -> str:
        return self._id

    def __str__(self) -> str:
        return f"📝 ({self.getId()}); Timestamp: {self.getTimestamp()}; Confidence: {self.getConfidence()};"

    def getTimestamp(self) -> int:
        return self._timestamp

    def getCandleSeries(self) -> ICandleSeries:
        return self._candleSeries

    def getConfidence(self) -> float:
        return self._confidence
