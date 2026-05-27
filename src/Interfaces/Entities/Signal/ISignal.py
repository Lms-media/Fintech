from abc import ABC, abstractmethod
from ..Prediction import IPrediction
from ..IEntity import IEntity

class ISignal(IEntity, ABC):

    @abstractmethod
    def getTimestamp(self) -> int:
        pass

    @abstractmethod
    def getPrediction(self) -> IPrediction:
        pass

    @abstractmethod
    def getVolume(self) -> float:
        pass
