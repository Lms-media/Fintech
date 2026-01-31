from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from Interfaces import IPrediction, ICandleSeries

P = TypeVar('P', bound=IPrediction)

class IPredictor(ABC, Generic[P]):

    @abstractmethod
    def predict(self, input: ICandleSeries) -> P:
        pass
