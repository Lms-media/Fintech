from abc import ABC, abstractmethod
from ...Primitives import TaskStatus, TaskType
from ...ValueObjects import ITaskTrigger, IAssetPair
from ..IEntity import IEntity

class ITask(IEntity, ABC):

    @abstractmethod
    def getStatus(self) -> TaskStatus:
        pass

    @abstractmethod
    def getType(self) -> TaskType:
        pass

    @abstractmethod
    def getTrigger(self) -> ITaskTrigger:
        pass

    @abstractmethod
    def getAssetPair(self) -> IAssetPair:
        pass

    @abstractmethod
    def getLotCount(self) -> int:
        pass

    @abstractmethod
    def unlock(self) -> None:
        pass

    @abstractmethod
    def finish(self) -> None:
        pass
    
    @abstractmethod
    def getTimestamp(self) -> int:
        pass
