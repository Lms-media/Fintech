from Interfaces import IPredictorAlgo, ICandleSeries
from .MAPredictorValue import MAPredictorValue
from Entities import PredictionMeta

class ExponentialMAPredictorAlgo(IPredictorAlgo[MAPredictorValue]):
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
        deltaSum = 0
        for i in range(self._candlesCount):
            candle = input.getByIndex(input.getCount() - 1 - i)
            if not candle:
                continue
            lastCloses.append(candle.getClosePrice())
            deltaSum += abs(candle.getOpenPrice() - candle.getClosePrice())

        if len(lastCloses) == 0:
            raise ValueError("No valid candles for EMA calculation")

        ema = lastCloses[0]
        alpha = 2.0 / (self._candlesCount + 1)
        for i in range(1, len(lastCloses)):
            ema = alpha * lastCloses[i] + (1 - alpha) * ema

        avgDelta = deltaSum / len(lastCloses)

        if ema > lastCandle.getClosePrice():
            return MAPredictorValue(meta, avgDelta)
        else:
            return MAPredictorValue(meta, -avgDelta)
