from abc import ABC, abstractmethod
from Interfaces import IMarket, IAction

class IExecutor(ABC):

    @abstractmethod
    def start(self, action: IAction) -> None:
        pass

    @abstractmethod
    def getMarket(self) -> IMarket:
        pass
