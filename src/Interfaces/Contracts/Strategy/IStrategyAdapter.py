from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from src.Interfaces import ISignal

V = TypeVar('V')
S = TypeVar('S', bound=ISignal)

class IStrategyAdapter(ABC, Generic[V, S]):

    @abstractmethod
    def convert(self, value: V) -> S:
        pass
