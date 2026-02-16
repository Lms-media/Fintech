from abc import abstractmethod
from typing import Generic, TypeVar
from Interfaces import IPredictorAlgo, ICandleSeries

V = TypeVar('V')

class ITrainablePredictorAlgo(IPredictorAlgo, Generic[V]):

    @abstractmethod
    def train(self, dataset: list[ICandleSeries]) -> None:
        pass
