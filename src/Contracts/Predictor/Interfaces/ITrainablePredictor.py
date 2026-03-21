from abc import abstractmethod
from typing import TypeVar
from Interfaces import IPredictor, IPrediction, IReadonlyCandleSeries

P = TypeVar('P', bound=IPrediction)

class ITrainablePredictor(IPredictor[P]):

    @abstractmethod
    def addDatasetItem(self, item: IReadonlyCandleSeries) -> None:
        pass

    @abstractmethod
    def train(self, epochs: int = 100) -> None:
        pass
