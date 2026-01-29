from abc import abstractmethod
from src.Primitives.Interfaces import IValueObject
from src.ValueObjects.Interfaces import IAsset

class IAssetPair(IValueObject['IAssetPair']):

    @abstractmethod
    def getBaseAsset(self) -> IAsset:
        pass

    @abstractmethod
    def getQuoteAsset(self) -> IAsset:
        pass

    @abstractmethod
    def withBaseAsset(self, baseAsset: IAsset) -> IAssetPair:
        pass

    @abstractmethod
    def withQuoteAsset(self, quoteAsset: IAsset) -> IAssetPair:
        pass
