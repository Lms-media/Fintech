from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from src.Interfaces import ICandleSeries

V = TypeVar('V')

class IPredictorAlgo(ABC, Generic[V]):

    @abstractmethod
    def calc(self, input: ICandleSeries) -> V:
        pass
