from abc import ABC
from Interfaces import ISignal, IPrediction

class ASignal(ISignal, ABC):
    _timestamp: int
    _prediction: IPrediction
    _volume: float

    def __init__(self, timestamp: int, prediction: IPrediction, volume: float):
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        self._timestamp = timestamp
        self._prediction = prediction
        self._volume = volume

    def getTimestamp(self):
        return self._timestamp

    def getPrediction(self):
        return self._prediction

    def getVolume(self):
        return self._volume
