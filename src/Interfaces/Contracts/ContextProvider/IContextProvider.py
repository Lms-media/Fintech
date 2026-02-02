from abc import ABC, abstractmethod
from Interfaces import IExecutionContext

class IContextProvider(ABC):

    @abstractmethod
    def getContext(self) -> IExecutionContext:
        pass
