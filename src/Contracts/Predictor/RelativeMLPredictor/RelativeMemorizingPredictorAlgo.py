from Interfaces import ICandleSeries
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo
from .RelativeMLPredictorValue import RelativeMLPredictorValue

class RelativeMemorizingPredictorAlgo(ITrainablePredictorAlgo[RelativeMLPredictorValue]):
    _candlesCount: int
    _memory: dict[str, list[float]]
    _maxAbsDelta: list[float]

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount
        self._memory = {}
        self._maxAbsDelta = [0.0, 0.0, 0.0, 0.0]

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
            return RelativeMLPredictorValue(meta, delta, self._maxAbsDelta)
        else:
            print(f"WARNING: Input not found in memory!")
            return RelativeMLPredictorValue(meta, [0.0, 0.0, 0.0, 0.0], self._maxAbsDelta)

    def train(self, dataset: list[ICandleSeries]) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        # Первый проход: собираем maxAbsDelta
        for series in dataset:
            count = series.getCount()
            lastCandle = series.getByIndex(count - 1)
            preLastCandle = series.getByIndex(count - 2)

            if not lastCandle or not preLastCandle:
                continue

            deltaOpen = abs(lastCandle.getOpenPrice() - preLastCandle.getOpenPrice())
            deltaClose = abs(lastCandle.getClosePrice() - preLastCandle.getClosePrice())
            deltaHigh = abs(lastCandle.getHighPrice() - preLastCandle.getHighPrice())
            deltaLow = abs(lastCandle.getLowPrice() - preLastCandle.getLowPrice())

            self._maxAbsDelta[0] = max(deltaOpen, self._maxAbsDelta[0])
            self._maxAbsDelta[1] = max(deltaClose, self._maxAbsDelta[1])
            self._maxAbsDelta[2] = max(deltaHigh, self._maxAbsDelta[2])
            self._maxAbsDelta[3] = max(deltaLow, self._maxAbsDelta[3])

        # Второй проход: нормализуем и запоминаем
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

            normDeltaOpen = deltaOpen / self._maxAbsDelta[0] if self._maxAbsDelta[0] != 0 else 0
            normDeltaClose = deltaClose / self._maxAbsDelta[1] if self._maxAbsDelta[1] != 0 else 0
            normDeltaHigh = deltaHigh / self._maxAbsDelta[2] if self._maxAbsDelta[2] != 0 else 0
            normDeltaLow = deltaLow / self._maxAbsDelta[3] if self._maxAbsDelta[3] != 0 else 0

            inputHash = self._hashSeries(series, self._candlesCount)
            self._memory[inputHash] = [normDeltaOpen, normDeltaClose, normDeltaHigh, normDeltaLow]
