from Interfaces import ICandleSeries
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo
from .RelativeMLPredictorValue import RelativeMLPredictorValue

class RelativeMemorizingPredictorAlgo(ITrainablePredictorAlgo[RelativeMLPredictorValue]):
    _candlesCount: int
    _memory: dict[str, list[float]]
    _deltaLimits: list[tuple[float, float]]

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount
        self._memory = {}
        self._deltaLimits = [
            (float('inf'), float('-inf')),
            (float('inf'), float('-inf')),
            (float('inf'), float('-inf')),
            (float('inf'), float('-inf')),
        ]

    def _hashSeries(self, series: ICandleSeries, count: int) -> str:
        """Create a hash from the first 'count' candles of the series."""
        parts = []
        for i in range(min(count, series.getCount())):
            candle = series.getByIndex(i)
            if candle:
                parts.append(f"{candle.getOpenTimestamp()}")
        return "|".join(parts)

    def calc(self, input: ICandleSeries):
        inputHash = self._hashSeries(input, self._candlesCount)

        lastCandle = input.getByIndex(input.getCount() - 1)
        if not lastCandle:
            raise ValueError("Cannot extract last candle from input")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        if inputHash in self._memory:
            delta = self._memory[inputHash]
            return RelativeMLPredictorValue(meta, delta, self._deltaLimits)
        else:
            print(f"WARNING: Input not found in memory!")
            return RelativeMLPredictorValue(meta, [0.5, 0.5, 0.5, 0.5], self._deltaLimits)

    def train(self, dataset: list[ICandleSeries]) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        for series in dataset:
            count = series.getCount()
            lastCandle = series.getByIndex(count - 1)
            preLastCandle = series.getByIndex(count - 2)

            if not lastCandle or not preLastCandle:
                continue

            deltaOpen = lastCandle.getOpenPrice() - preLastCandle.getOpenPrice()
            deltaClose = lastCandle.getClosePrice() - preLastCandle.getClosePrice()
            deltaHigh = lastCandle.getHighPrice() - preLastCandle.getHighPrice()
            deltaLow = lastCandle.getLowPrice() - preLastCandle.getLowPrice()

            self._deltaLimits[0] = (min(deltaOpen, self._deltaLimits[0][0]), max(deltaOpen, self._deltaLimits[0][1]))
            self._deltaLimits[1] = (min(deltaClose, self._deltaLimits[1][0]), max(deltaClose, self._deltaLimits[1][1]))
            self._deltaLimits[2] = (min(deltaHigh, self._deltaLimits[2][0]), max(deltaHigh, self._deltaLimits[2][1]))
            self._deltaLimits[3] = (min(deltaLow, self._deltaLimits[3][0]), max(deltaLow, self._deltaLimits[3][1]))

        for series in dataset:
            count = series.getCount()
            lastCandle = series.getByIndex(count - 1)
            preLastCandle = series.getByIndex(count - 2)

            if not lastCandle or not preLastCandle:
                continue

            deltaOpen = lastCandle.getOpenPrice() - preLastCandle.getOpenPrice()
            deltaClose = lastCandle.getClosePrice() - preLastCandle.getClosePrice()
            deltaHigh = lastCandle.getHighPrice() - preLastCandle.getHighPrice()
            deltaLow = lastCandle.getLowPrice() - preLastCandle.getLowPrice()

            # Normalize delta
            normDeltaOpen = (deltaOpen - self._deltaLimits[0][0]) / (self._deltaLimits[0][1] - self._deltaLimits[0][0])
            normDeltaClose = (deltaClose - self._deltaLimits[1][0]) / (self._deltaLimits[1][1] - self._deltaLimits[1][0])
            normDeltaHigh = (deltaHigh - self._deltaLimits[2][0]) / (self._deltaLimits[2][1] - self._deltaLimits[2][0])
            normDeltaLow = (deltaLow - self._deltaLimits[3][0]) / (self._deltaLimits[3][1] - self._deltaLimits[3][0])

            inputHash = self._hashSeries(series, self._candlesCount)
            self._memory[inputHash] = [normDeltaOpen, normDeltaClose, normDeltaHigh, normDeltaLow]
