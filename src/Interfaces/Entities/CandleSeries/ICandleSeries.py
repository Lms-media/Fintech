from abc import abstractmethod
from typing import Optional
from ...ValueObjects import ICandle
from .IReadonlyCandleSeries import IReadonlyCandleSeries

class ICandleSeries(IReadonlyCandleSeries):

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
