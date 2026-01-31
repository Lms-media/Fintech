from abc import abstractmethod
from Interfaces import IExecutionContext, IValueObject

class ITaskTrigger(IValueObject['ITaskTrigger']):

    @abstractmethod
    def isTriggered(self, context: IExecutionContext) -> bool:
        pass
