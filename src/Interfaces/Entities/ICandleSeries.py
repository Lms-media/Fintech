from abc import ABC, abstractmethod
from typing import Optional
from src.ValueObjects.Interfaces.ICandle import ICandle

class ICandleSeries(ABC):

    @abstractmethod
    def getCount(self) -> int:
        pass

    @abstractmethod
    def getByIndex(self, index: int) -> Optional[ICandle]:
        pass

    @abstractmethod
    def getByTimestamp(self, timestamp: int) -> Optional[ICandle]:
        pass
