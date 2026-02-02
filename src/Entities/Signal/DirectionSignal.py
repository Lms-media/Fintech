from .Interfaces import IDirectionSignal
from Interfaces import IPrediction, DirectionType
from Entities import APrediction

class DirectionSignal(APrediction, IDirectionSignal):
    _direction: DirectionType

    def __init__(self, timestamp: int, prediction: IPrediction, volume: float, direction: DirectionType):
        super().__init__(timestamp, prediction, volume)
        self._direction = direction

    def getDirection(self) -> DirectionType:
        return self._direction
