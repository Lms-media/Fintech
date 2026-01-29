from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from src.Interfaces import IPrediction

P = TypeVar('P', bound=IPrediction)
V = TypeVar('V')

class IStrategyAlgo(ABC, Generic[P, V]):

    @abstractmethod
    def calc(self, input: P) -> V:
        pass
