from abc import ABC, abstractmethod
from typing import Optional
from ...ValueObjects import IExecutionContext
from Interfaces import ICandle

class IContextProvider(ABC):

    @abstractmethod
    def getContext(self, timestamp: int) -> IExecutionContext:
        pass

    @abstractmethod
    def getNextCandle(self, candle: ICandle) -> Optional[ICandle]:
        pass
