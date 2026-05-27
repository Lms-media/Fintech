from abc import ABC, abstractmethod
from ...Services import IMarket
from ...Entities import IAction

class IExecutor(ABC):

    @abstractmethod
    def start(self, action: IAction) -> None:
        pass
