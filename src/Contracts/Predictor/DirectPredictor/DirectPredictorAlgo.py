from Interfaces import IPredictorAlgo, ICandleSeries
from .DirectPredictorValue import DirectPredictorValue
from Entities import PredictionMeta, TrimmedCandleSeries, CandleSeries
from copy import deepcopy

class DirectPredictorAlgo(IPredictorAlgo[DirectPredictorValue]):

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from input")

        return DirectPredictorValue(
            PredictionMeta(
                lastCandle.getOpenTimestamp(),
                input,
                1.0
            ), 
            lastCandle
        )
