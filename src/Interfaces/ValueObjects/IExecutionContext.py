from abc import abstractmethod
from src.Interfaces import IAssetPair, IValueObject

class IExecutionContext(IValueObject['IExecutionContext']):

    @abstractmethod
    def getCurrentPrice(self, assetPair: IAssetPair) -> float:
        pass

    @abstractmethod
    def getTimestamp(self) -> int:
        pass
