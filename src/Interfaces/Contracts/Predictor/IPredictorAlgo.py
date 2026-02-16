from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from ...Entities import IReadonlyCandleSeries

V = TypeVar('V')

class IPredictorAlgo(ABC, Generic[V]):

    @abstractmethod
    def calc(self, input: IReadonlyCandleSeries) -> V:
        pass
