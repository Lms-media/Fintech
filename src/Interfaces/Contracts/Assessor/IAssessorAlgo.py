from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from Interfaces import ISignal

S = TypeVar('S', bound=ISignal)
V = TypeVar('V')

class IAssessorAlgo(ABC, Generic[V]):

    @abstractmethod
    def calc(self, input: S) -> V:
        pass
