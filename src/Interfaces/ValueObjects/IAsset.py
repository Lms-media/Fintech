from abc import abstractmethod
from Interfaces import IValueObject

class IAsset(IValueObject['IAsset']):

    @abstractmethod
    def getTickerCode(self) -> str:
        pass

    @abstractmethod
    def getLotSize(self) -> int:
        pass

    @abstractmethod
    def withTickerCode(self, tickerCode: str) -> IAsset:
        pass

    @abstractmethod
    def withLotSize(self, lotSize: int) -> IAsset:
        pass
