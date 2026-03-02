from abc import ABC, abstractmethod
from typing import List
from ...Primitives import ActionStatus
from ...ValueObjects import IExecutionContext
from ..Signal import ISignal
from ..Task import ITask
from ..IEntity import IEntity

class   IAction(IEntity, ABC):

    @abstractmethod
    def getSignal(self) -> ISignal:
        pass

    @abstractmethod
    def getStatus(self) -> ActionStatus:
        pass

    @abstractmethod
    def getTasks(self) -> List[ITask]:
        pass

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def finish(self) -> None:
        pass

    @abstractmethod
    def update(self, context: IExecutionContext) -> None:
        pass
