from .Interfaces import IDirectionSignal
from Interfaces import IPrediction, DirectionType
from .ASignal import ASignal

class DirectionSignal(ASignal, IDirectionSignal):
    _direction: DirectionType

    def __init__(self, timestamp: int, prediction: IPrediction, volume: float, direction: DirectionType):
        super().__init__(timestamp, prediction, volume)
        self._direction = direction

    def getDirection(self) -> DirectionType:
        return self._direction

    def __str__(self) -> str:
        return f"{super().__str__()} Direction: {self.getDirection()}"
