from abc import ABC
import uuid
from Interfaces import ISignal, IPrediction

class ASignal(ISignal, ABC):  # pragma: no cover
    _id: str
    _timestamp: int
    _prediction: IPrediction
    _volume: float

    def __init__(self, timestamp: int, prediction: IPrediction, volume: float):
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        self._timestamp = timestamp
        self._prediction = prediction
        self._volume = volume
        self._id = str(uuid.uuid4())

    def getId(self) -> str:
        return self._id

    def getTimestamp(self):
        return self._timestamp

    def getPrediction(self):
        return self._prediction

    def getVolume(self):
        return self._volume

    def __str__(self) -> str:
        return f"🪧 ({self.getId()}); Timestamp: {self.getTimestamp()}; Volume: {self.getVolume()};"
