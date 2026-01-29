from abc import ABC, abstractmethod
from src.Entities.Interfaces.IPrediction import IPrediction

class ISignal(ABC):

    @abstractmethod
    def getTimestamp(self) -> int:
        pass

    @abstractmethod
    def getPrediction(self) -> IPrediction:
        pass

    @abstractmethod
    def getVolume(self) -> float:
        pass
