from abc import abstractmethod
from Primitives.IValueObject import IValueObject
from src.ValueObjects.Interfaces.IAssetPair import IAssetPair

class IExecutionContext(IValueObject['IExecutionContext']):

    @abstractmethod
    def getCurrentPrice(self, assetPair: IAssetPair) -> float:
        pass

    @abstractmethod
    def getTimestamp(self) -> int:
        pass
