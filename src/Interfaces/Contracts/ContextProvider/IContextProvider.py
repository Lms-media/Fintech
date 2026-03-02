from abc import ABC, abstractmethod
from ...ValueObjects import IExecutionContext

class IContextProvider(ABC):

    @abstractmethod
    def getContext(self, timestamp: int) -> IExecutionContext:
        pass
