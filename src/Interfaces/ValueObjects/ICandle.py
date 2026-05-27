from __future__ import annotations
from abc import abstractmethod
from ..Primitives import IntervalType, IValueObject
from .IAssetPair import IAssetPair

class ICandle(IValueObject['ICandle']):

    @abstractmethod
    def getAssetPair(self) -> IAssetPair:
        pass

    @abstractmethod
    def getOpenTimestamp(self) -> int:
        pass

    @abstractmethod
    def getInterval(self) -> IntervalType:
        pass

    @abstractmethod
    def getOpenPrice(self) -> float:
        pass

    @abstractmethod
    def getClosePrice(self) -> float:
        pass

    @abstractmethod
    def getHighPrice(self) -> float:
        pass

    @abstractmethod
    def getLowPrice(self) -> float:
        pass

    @abstractmethod
    def getVolume(self) -> float:
        pass

    @abstractmethod
    def withAssetPair(self, assetPair: IAssetPair) -> ICandle:
        pass

    @abstractmethod
    def withOpenTimestamp(self, openTimestamp: int) -> ICandle:
        pass

    @abstractmethod
    def withInterval(self, interval: IntervalType) -> ICandle:
        pass

    @abstractmethod
    def withOpenPrice(self, openPrice: float) -> ICandle:
        pass

    @abstractmethod
    def withClosePrice(self, closePrice: float) -> ICandle:
        pass

    @abstractmethod
    def withHighPrice(self, highPrice: float) -> ICandle:
        pass

    @abstractmethod
    def withLowPrice(self, lowPrice: float) -> ICandle:
        pass

    @abstractmethod
    def withVolume(self, volume: float) -> ICandle:
        pass
