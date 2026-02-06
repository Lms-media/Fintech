from abc import ABC, abstractmethod
from typing import Optional
from ...ValueObjects import ICandle, IAssetPair
from ..IEntity import IEntity

class ICandleSeries(IEntity, ABC):

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
    def appendLeft(self, candle: ICandle) -> None:
        pass

    @abstractmethod
    def appendRight(self, candle: ICandle) -> None:
        pass

    @abstractmethod
    def popLeft(self) -> Optional[ICandle]:
        pass

    @abstractmethod
    def popRight(self) -> Optional[ICandle]:
        pass
