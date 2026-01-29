from abc import ABC, abstractmethod
from src.Interfaces import IPrediction

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
