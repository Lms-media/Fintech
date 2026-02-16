from Interfaces import IPredictorAlgo, ICandleSeries
from .DummyPredictorValue import DummyPredictorValue
from Entities import PredictionMeta

class DummyPredictorAlgo(IPredictorAlgo[DummyPredictorValue]):

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from input")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        return DummyPredictorValue(meta, 10, 1)
