from abc import ABC, abstractmethod
from src.Interfaces import ITask

class IMarket(ABC):

    @abstractmethod
    def execute(self, task: ITask) -> None:
        pass
