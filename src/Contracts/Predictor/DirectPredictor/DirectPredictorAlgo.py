from Interfaces import IPredictorAlgo, ICandleSeries
from .DirectPredictorValue import DirectPredictorValue
from Entities import PredictionMeta
from copy import deepcopy

class DirectPredictorAlgo(IPredictorAlgo[DirectPredictorValue]):

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)
        metaCandles = deepcopy(input)
        metaCandles.popRight()

        if not lastCandle:
            raise ValueError("Cannot extract last candle from input")

        timestamp = lastCandle.getOpenTimestamp()

        return DirectPredictorValue(
            PredictionMeta(timestamp, metaCandles, 1.0), 
            lastCandle
        )
