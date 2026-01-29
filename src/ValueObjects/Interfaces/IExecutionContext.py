from abc import abstractmethod
from src.Primitives.Interfaces import IValueObject
from src.ValueObjects.Interfaces import IAssetPair

class IExecutionContext(IValueObject['IExecutionContext']):

    @abstractmethod
    def getCurrentPrice(self, assetPair: IAssetPair) -> float:
        pass

    @abstractmethod
    def getTimestamp(self) -> int:
        pass
