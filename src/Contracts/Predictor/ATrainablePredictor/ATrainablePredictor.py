from typing import Generic, TypeVar, Optional
from ..APredictor import APredictor
from ..Interfaces import ITrainablePredictor, ITrainablePredictorAlgo
from Interfaces import ICandleSeries, IPrediction, IPredictorAdapter

V = TypeVar('V')
P = TypeVar('P', bound=IPrediction)

class ATrainablePredictor(APredictor[V, P], ITrainablePredictor, Generic[V, P]):
    _algo: ITrainablePredictorAlgo[V]
    _dataset: list[ICandleSeries]

    def __init__(self, algo: ITrainablePredictorAlgo[V], adapter: IPredictorAdapter[V, P], candlesCount: int):
        super().__init__(algo, adapter, candlesCount)
        self._candlesCount = candlesCount
        self._dataset = list()
        self._algo = algo

    def addDatasetItem(self, item: ICandleSeries):
        if item.getCount() != self._candlesCount + 1:
            raise ValueError(f"Incorrect dataset item size")
        self._dataset.append(item)

    def clearDataset(self):
        self._dataset = list()

    def train(self, epochs: int = 100, datasetItemLimit: Optional[int] = None):
        if datasetItemLimit is not None:
            limitedDataset = self._dataset[-datasetItemLimit:]
        else:
            limitedDataset = self._dataset
        self._algo.train(limitedDataset, epochs)
