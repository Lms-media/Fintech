from abc import ABC, abstractmethod
from ..IEntity import IEntity
from ..PredictionMeta import IPredictionMeta

class IPrediction(IEntity, ABC):

    @abstractmethod
    def getMeta(self) -> IPredictionMeta:
        pass
