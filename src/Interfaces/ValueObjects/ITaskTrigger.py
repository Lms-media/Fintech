from abc import abstractmethod
from Primitives.IValueObject import IValueObject
from src.ValueObjects.Interfaces.IExecutionContext import IExecutionContext

class ITaskTrigger(IValueObject['ITaskTrigger']):

    @abstractmethod
    def isTriggered(self, context: IExecutionContext) -> bool:
        pass
