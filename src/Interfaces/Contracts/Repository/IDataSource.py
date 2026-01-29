from abc import ABC, abstractmethod
from src.Interfaces import ICandleSeries

class IDataSource(ABC):

    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def getSeries(self) -> ICandleSeries:
        pass
