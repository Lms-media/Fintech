from abc import ABC, abstractmethod
from Interfaces import ITask

class IMarket(ABC):

    @abstractmethod
    def execute(self, task: ITask) -> None:
        pass
