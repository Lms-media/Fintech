from abc import abstractmethod
from Interfaces import IPrediction, DirectionType

class IDirectionSignal(IPrediction):

    @abstractmethod
    def getDirection(self) -> DirectionType:
        pass
