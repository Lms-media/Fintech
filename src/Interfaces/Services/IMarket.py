from abc import ABC, abstractmethod
from ..Entities import ITask

class IMarket(ABC):

    @abstractmethod
    def execute(self, task: ITask) -> None:
        pass
