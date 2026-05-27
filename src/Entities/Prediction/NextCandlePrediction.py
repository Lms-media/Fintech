from Interfaces import ICandle, IPredictionMeta
from .Interfaces import INextCandlePrediction
from .APrediction import APrediction

class NextCandlePrediction(APrediction, INextCandlePrediction):
    _nextCandle: ICandle

    def __init__(self, meta: IPredictionMeta, nextCandle: ICandle):
        super().__init__(meta)
        self._nextCandle = nextCandle

    def getNextCandle(self) -> ICandle:
        return self._nextCandle

    def __str__(self) -> str:
        return f"{super().__str__()} Next candle: {self.getNextCandle()}"
