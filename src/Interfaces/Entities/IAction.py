from abc import ABC, abstractmethod
from typing import List
from src.Interfaces import ActionStatus, ISignal, ITask

class IAction(ABC):

    @abstractmethod
    def getSignal(self) -> ISignal:
        pass

    @abstractmethod
    def getStatus(self) -> ActionStatus:
        pass

    @abstractmethod
    def getTasks(self) -> List[ITask]:
        pass
