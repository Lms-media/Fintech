from abc import ABC, abstractmethod
from ..CandleSeries import ICandleSeries

class IPrediction(ABC):

    @abstractmethod
    def getTimestamp(self) -> int:
        pass

    @abstractmethod
    def getCandleSeries(self) -> ICandleSeries:
        pass

    @abstractmethod
    def getConfidence(self) -> float:
        pass
