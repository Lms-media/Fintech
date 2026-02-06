from abc import ABC, abstractmethod
from ..CandleSeries import ICandleSeries
from ..IEntity import IEntity

class IPrediction(IEntity, ABC):

    @abstractmethod
    def getTimestamp(self) -> int:
        pass

    @abstractmethod
    def getCandleSeries(self) -> ICandleSeries:
        pass

    @abstractmethod
    def getConfidence(self) -> float:
        pass
