from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from Interfaces import ISignal, IAction

S = TypeVar('S', bound=ISignal)
A = TypeVar('A', bound=IAction)

class IAssessor(ABC, Generic[S, A]):

    @abstractmethod
    def getAction(self, input: S) -> A:
        pass
