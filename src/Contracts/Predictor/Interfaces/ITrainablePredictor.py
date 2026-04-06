from abc import abstractmethod
from typing import TypeVar, Optional
from Interfaces import IPredictor, IPrediction, IReadonlyCandleSeries

P = TypeVar('P', bound=IPrediction)

class ITrainablePredictor(IPredictor[P]):

    @abstractmethod
    def addDatasetItem(self, item: IReadonlyCandleSeries) -> None:
        pass

    @abstractmethod
    def clearDataset(self) -> None:
        pass

    @abstractmethod
    def train(self, epochs: int = 100, datasetItemLimit: Optional[int] = None) -> None:
        pass
