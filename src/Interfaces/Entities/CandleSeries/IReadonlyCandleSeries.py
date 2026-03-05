from abc import ABC, abstractmethod
from typing import Optional
from ...ValueObjects import ICandle, IAssetPair
from ..IEntity import IEntity

class IReadonlyCandleSeries(IEntity, ABC):

    @abstractmethod
    def getCount(self) -> int:
        pass

    @abstractmethod
    def getAssetPair(self) -> IAssetPair:
        pass

    @abstractmethod
    def getByIndex(self, index: int) -> Optional[ICandle]:
        pass

    @abstractmethod
    def getByTimestamp(self, timestamp: int) -> Optional[ICandle]:
        pass
    
    @abstractmethod
    def getIndexOf(self, candle: ICandle) -> int:
        pass
