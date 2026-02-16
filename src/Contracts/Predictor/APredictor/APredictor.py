from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from Interfaces import IPredictor, IPredictorAlgo, IPredictorAdapter, IPrediction, IReadonlyCandleSeries

V = TypeVar('V')
P = TypeVar('P', bound=IPrediction)

class APredictor(IPredictor[P], Generic[V, P]):
    _algo: IPredictorAlgo[V]
    _adapter: IPredictorAdapter[V, P]

    def __init__(self, algo: IPredictorAlgo[V], adapter: IPredictorAdapter[V, P]):
        self._algo = algo
        self._adapter = adapter

    def predict(self, input: IReadonlyCandleSeries) -> P:
        value = self._algo.calc(input)
        prediction = self._adapter.transform(value)

        return prediction
