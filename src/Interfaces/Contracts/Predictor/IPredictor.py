from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from ...Entities import IPrediction, IReadonlyCandleSeries

P = TypeVar('P', bound=IPrediction)

class IPredictor(ABC, Generic[P]):

    def usesCurrentOpen(self) -> bool:
        """True for predictors of current Close[n] given known Open[n]."""
        return False

    @abstractmethod
    def predict(self, input: IReadonlyCandleSeries) -> P:
        pass

    @abstractmethod
    def getCandlesCount(self) -> int:
        pass
