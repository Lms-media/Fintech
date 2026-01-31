from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from Interfaces import IPrediction, ISignal

P = TypeVar('P', bound=IPrediction)
S = TypeVar('S', bound=ISignal)

class IStrategy(ABC, Generic[P, S]):

    @abstractmethod
    def getSignal(self, input: P) -> S:
        pass
