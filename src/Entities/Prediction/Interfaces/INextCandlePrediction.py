from abc import abstractmethod
from Interfaces import ICandle, IPrediction

class INextCandlePrediction(IPrediction):

    @abstractmethod
    def getNextCandle(self) -> ICandle:
        pass
