from abc import abstractmethod
from src.ValueObjects.Interfaces import IExecutionContext
from src.Primitives.Interfaces import IValueObject

class ITaskTrigger(IValueObject['ITaskTrigger']):

    @abstractmethod
    def isTriggered(self, context: IExecutionContext) -> bool:
        pass
