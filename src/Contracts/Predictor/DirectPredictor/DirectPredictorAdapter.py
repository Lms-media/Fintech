from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .DirectPredictorValue import DirectPredictorValue


class DirectPredictorAdapter(IPredictorAdapter[DirectPredictorValue, INextCandlePrediction]):
    def transform(self, value: DirectPredictorValue) -> INextCandlePrediction:
        return NextCandlePrediction(value.meta, value.nextCandle)