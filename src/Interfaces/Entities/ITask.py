from abc import ABC, abstractmethod
from src.Primitives.Enums.TaskStatus import TaskStatus
from src.Primitives.Enums.TaskType import TaskType
from src.ValueObjects.Interfaces.ITaskTrigger import ITaskTrigger

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
    def unlock(self) -> None:
        pass

    @abstractmethod
    def finish(self) -> None:
        pass
