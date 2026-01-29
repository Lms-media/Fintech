from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from src.Interfaces import IAction

V = TypeVar('V')
A = TypeVar('A', bound=IAction)

class IAssessorAdapter(ABC, Generic[V, A]):

    @abstractmethod
    def convert(self, value: V) -> A:
        pass
