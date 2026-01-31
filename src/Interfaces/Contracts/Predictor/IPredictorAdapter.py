from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from Interfaces import IPrediction

V = TypeVar('V')
P = TypeVar('P', bound=IPrediction)

class IPredictorAdapter(ABC, Generic[V, P]):

    @abstractmethod
    def convert(self, value: V) -> P:
        pass
