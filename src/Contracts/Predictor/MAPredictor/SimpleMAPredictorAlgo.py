from Interfaces import IPredictorAlgo, ICandleSeries
from .MAPredictorValue import MAPredictorValue
from Entities import PredictionMeta

class SimpleMAPredictorAlgo(IPredictorAlgo[MAPredictorValue]):
    _candlesCount: int

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 2)
        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        lastOpens = []
        for i in range(self._candlesCount):
            candle = input.getByIndex(input.getCount() - 2 - i)
            lastOpens.append(candle.getOpenPrice())

        average = sum(lastOpens) / len(lastOpens)

        return MAPredictorValue(meta, average)
