from abc import ABC, abstractmethod
from ..IEntity import IEntity
from ..CandleSeries import ICandleSeries

class IPredictionMeta(IEntity, ABC):

    @abstractmethod
    def getTimestamp(self) -> int:
        pass

    @abstractmethod
    def getCandleSeries(self) -> ICandleSeries:
        pass

    @abstractmethod
    def getConfidence(self) -> float:
        pass
