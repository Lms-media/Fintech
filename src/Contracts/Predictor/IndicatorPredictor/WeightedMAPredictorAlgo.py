from Interfaces import IPredictorAlgo, ICandleSeries
from .IndicatorPredictorValue import IndicatorPredictorValue
from Entities import PredictionMeta

class WeightedMAPredictorAlgo(IPredictorAlgo[IndicatorPredictorValue]):
    _candlesCount: int

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        lastCloses = []
        multiplier = 1
        multSum = 0
        deltaSum = 0
        for i in range(self._candlesCount):
            candle = input.getByIndex(input.getCount() - 1 - i)
            if not candle:
                continue
            lastCloses.append(multiplier * candle.getClosePrice())
            multSum += multiplier
            multiplier += 1
            deltaSum += abs(candle.getOpenPrice() - candle.getClosePrice())

        average = sum(lastCloses) / multSum
        avgDelta = deltaSum / len(lastCloses)

        if abs(average - lastCandle.getClosePrice()) < 0.1:
            return IndicatorPredictorValue(meta, 0)

        if average > lastCandle.getClosePrice():
            return IndicatorPredictorValue(meta, -avgDelta)
        else:
            return IndicatorPredictorValue(meta, avgDelta)
