from abc import ABC, abstractmethod
from src.Interfaces import TaskStatus, TaskType, ITaskTrigger, IAssetPair

class ITask(ABC):

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
