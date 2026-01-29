from abc import ABC, abstractmethod
from typing import List
from src.Primitives.Enums.ActionStatus import ActionStatus
from src.Entities.Interfaces.ISignal import ISignal
from src.Entities.Interfaces.ITask import ITask

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
