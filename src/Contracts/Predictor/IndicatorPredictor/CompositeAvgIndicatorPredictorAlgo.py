from Interfaces import IPredictorAlgo, ICandleSeries
from .IndicatorPredictorValue import IndicatorPredictorValue
from Entities import PredictionMeta

class CompositeAvgIndicatorPredictorAlgo(IPredictorAlgo[IndicatorPredictorValue]):
    _algos: list[IPredictorAlgo[IndicatorPredictorValue]]

    def __init__(self, algos: list[IPredictorAlgo[IndicatorPredictorValue]]):
        self._algos = algos

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        if len(self._algos) == 0:
            return IndicatorPredictorValue(meta, 0)

        totalDelta = 0
        for algo in self._algos:
            result = algo.calc(input)
            totalDelta += result.priceDelta

        avgDelta = totalDelta / len(self._algos)

        return IndicatorPredictorValue(meta, avgDelta)
