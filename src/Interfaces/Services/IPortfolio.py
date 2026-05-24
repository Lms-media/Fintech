from abc import ABC, abstractmethod

from Interfaces import IExecutionContext
from ..ValueObjects import IAsset

class IPortfolio(ABC):

    @abstractmethod
    def getBaseAsset(self) -> IAsset:
        pass

    @abstractmethod
    def getAssetAmount(self, asset: IAsset) -> float:
        pass

    @abstractmethod
    def getBaseAmount(self) -> float:
        pass

    @abstractmethod
    def getCapitalization(self, context: IExecutionContext) -> float:
        pass

    @abstractmethod
    def buyAsset(self, asset: IAsset, lotCount: int, price: float) -> None:
        pass

    @abstractmethod
    def sellAsset(self, asset: IAsset, lotCount: int, price: float) -> None:
        pass

    @abstractmethod
    def deposit(self, amount: float) -> None:
        pass

    @abstractmethod
    def withdraw(self, amount: float) -> None:
        pass
