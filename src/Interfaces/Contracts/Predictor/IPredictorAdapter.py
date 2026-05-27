from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from ...Entities import IPrediction

V = TypeVar('V')
P = TypeVar('P', bound=IPrediction)

class IPredictorAdapter(ABC, Generic[V, P]):

    @abstractmethod
    def transform(self, value: V) -> P:
        pass
