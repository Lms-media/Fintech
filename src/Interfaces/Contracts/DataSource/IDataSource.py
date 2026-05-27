from abc import ABC, abstractmethod
from ...Entities import ICandleSeries

class IDataSource(ABC):

    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def getSeries(self) -> ICandleSeries:
        pass
