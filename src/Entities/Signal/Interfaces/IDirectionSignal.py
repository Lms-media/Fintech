from abc import abstractmethod
from Interfaces import ISignal, DirectionType

class IDirectionSignal(ISignal):

    @abstractmethod
    def getDirection(self) -> DirectionType:
        pass
