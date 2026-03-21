from abc import abstractmethod
from typing import Generic, TypeVar
from Interfaces import IPredictorAlgo, ICandleSeries

V = TypeVar('V')

class ITrainablePredictorAlgo(IPredictorAlgo, Generic[V]):

    @abstractmethod
    def train(self, dataset: list[ICandleSeries], epochs: int = 100) -> None:
        pass
