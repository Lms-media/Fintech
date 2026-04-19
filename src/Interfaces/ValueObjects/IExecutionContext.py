from __future__ import annotations
from abc import abstractmethod
from typing import Optional
from ..Primitives import IValueObject
from .IAssetPair import IAssetPair

class IExecutionContext(IValueObject['IExecutionContext']):

    @abstractmethod
    def getPrice(self, assetPair: IAssetPair) -> Optional[float]:
        pass
    
    @abstractmethod
    def getClosePrice(self, assetPair: IAssetPair) -> Optional[float]:
        pass
    
    @abstractmethod
    def getHightPrice(self, assetPair: IAssetPair) -> Optional[float]:
        pass
    
    @abstractmethod
    def getLowPrice(self, assetPair: IAssetPair) -> Optional[float]:
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
