from abc import abstractmethod
from typing import Optional
from Interfaces import IAssetPair, IValueObject

class IExecutionContext(IValueObject['IExecutionContext']):

    @abstractmethod
    def getPrice(self, assetPair: IAssetPair) -> Optional[float]:
        pass

    @abstractmethod
    def getTimestamp(self) -> int:
        pass

    @abstractmethod
    def withPrice(self, assetPair: IAssetPair) -> IExecutionContext:
        pass

    @abstractmethod
    def withTimestamp(self, timestamp: int) -> IExecutionContext:
        pass
