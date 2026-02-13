from Interfaces import IPredictorAlgo, ICandleSeries
from .MAPredictorValue import MAPredictorValue
from Entities import PredictionMeta

class WeightedMAPredictorAlgo(IPredictorAlgo[MAPredictorValue]):
    _candlesCount: int

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)
        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        lastOpens = []
        multiplier = 1
        multSum = 0
        deltaSum = 0
        for i in range(self._candlesCount):
            candle = input.getByIndex(input.getCount() - 1 - i)
            if not candle:
                continue
            lastOpens.append(multiplier * candle.getOpenPrice())
            multSum += multiplier
            multiplier += 1
            deltaSum += abs(candle.getOpenPrice() - candle.getClosePrice())

        average = sum(lastOpens) / multSum
        avgDelta = deltaSum / len(lastOpens)

        if average > lastCandle.getOpenPrice():
            return MAPredictorValue(meta, -avgDelta)
        else:
            return MAPredictorValue(meta, avgDelta)
