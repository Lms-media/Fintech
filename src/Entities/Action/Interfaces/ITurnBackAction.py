from abc import ABC, abstractmethod
from Interfaces import IAction

class ITurnBackAction(IAction, ABC):

    @abstractmethod
    def getDuration(self) -> int:
        pass
