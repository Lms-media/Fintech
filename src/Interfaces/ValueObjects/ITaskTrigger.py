from __future__ import annotations
from abc import abstractmethod
from ..Primitives import IValueObject
from .IExecutionContext import IExecutionContext

class ITaskTrigger(IValueObject['ITaskTrigger']):

    @abstractmethod
    def isTriggered(self, context: IExecutionContext) -> bool:
        pass
