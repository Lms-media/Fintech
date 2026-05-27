import numpy as np
from keras.models import Sequential
from keras.layers import Dense, Dropout, SimpleRNN
from keras.optimizers import Adam
from Interfaces import ICandleSeries
from .RelativeMLPredictorValue import RelativeMLPredictorValue
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo

class RelativeRNNPredictorAlgo(ITrainablePredictorAlgo[RelativeMLPredictorValue]):
    _candlesCount: int
    _maxAbsDelta: list[float]

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount
        self._maxAbsDelta = [0.0, 0.0, 0.0, 0.0]

        self._model = Sequential([
            SimpleRNN(50, return_sequences=False, input_shape=(candlesCount, 4)),
            Dense(25, activation='relu'),
            Dropout(0.2),
            Dense(4, activation='linear')
        ])
        self._model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )

    def calc(self, input: ICandleSeries):
        allNormalized = self._normalize(input)
        normalized = allNormalized[-self._candlesCount:]
        normalized = np.array([normalized])
        normalized = normalized.reshape(1, self._candlesCount, 4)
        output = self._model.predict(normalized, verbose='0')
        result = output[0]

        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        return RelativeMLPredictorValue(meta, result, self._maxAbsDelta)

    def train(self, dataset: list[ICandleSeries], epochs: int = 100) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        self._initNormalization(dataset)

        X_list = []
        y_list = []

        for series in dataset:
            normalized = self._normalize(series)
            X_list.append(normalized[:self._candlesCount])
            y_list.append(self._getTargetDelta(series))

        X_train = np.array(X_list)
        y_train = np.array(y_list)

        self._model.fit(X_train, y_train, epochs=epochs, batch_size=32, validation_split=0.2, verbose='auto')

    def _getTargetDelta(self, series: ICandleSeries) -> list[float]:
        count = series.getCount()
        lastCandle = series.getByIndex(count - 1)
        prevCandle = series.getByIndex(count - 2)

        if not lastCandle or not prevCandle:
            return [0, 0, 0, 0]

        deltaOpen = lastCandle.getOpenPrice() - prevCandle.getOpenPrice()
        deltaClose = lastCandle.getClosePrice() - prevCandle.getClosePrice()
        deltaHigh = lastCandle.getHighPrice() - prevCandle.getHighPrice()
        deltaLow = lastCandle.getLowPrice() - prevCandle.getLowPrice()

        scaledOpen = deltaOpen / self._maxAbsDelta[0] if self._maxAbsDelta[0] != 0 else 0
        scaledClose = deltaClose / self._maxAbsDelta[1] if self._maxAbsDelta[1] != 0 else 0
        scaledHigh = deltaHigh / self._maxAbsDelta[2] if self._maxAbsDelta[2] != 0 else 0
        scaledLow = deltaLow / self._maxAbsDelta[3] if self._maxAbsDelta[3] != 0 else 0

        return [scaledOpen, scaledClose, scaledHigh, scaledLow]

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
            count = item.getCount()

            for i in range(1, count):
                candle = item.getByIndex(i)
                prevCandle = item.getByIndex(i - 1)

                if not candle or not prevCandle:
                    continue

                deltaOpen = abs(candle.getOpenPrice() - prevCandle.getOpenPrice())
                deltaClose = abs(candle.getClosePrice() - prevCandle.getClosePrice())
                deltaHigh = abs(candle.getHighPrice() - prevCandle.getHighPrice())
                deltaLow = abs(candle.getLowPrice() - prevCandle.getLowPrice())

                self._maxAbsDelta[0] = max(deltaOpen, self._maxAbsDelta[0])
                self._maxAbsDelta[1] = max(deltaClose, self._maxAbsDelta[1])
                self._maxAbsDelta[2] = max(deltaHigh, self._maxAbsDelta[2])
                self._maxAbsDelta[3] = max(deltaLow, self._maxAbsDelta[3])

    def _normalize(self, input: ICandleSeries) -> list[list[float]]:
        count = input.getCount()
        normalized = list[list[float]]()

        normalized.append([0, 0, 0, 0])

        for i in range(1, count):
            candle = input.getByIndex(i)
            prevCandle = input.getByIndex(i - 1)

            if not candle or not prevCandle:
                normalized.append([0, 0, 0, 0])
                continue

            deltaOpen = candle.getOpenPrice() - prevCandle.getOpenPrice()
            deltaClose = candle.getClosePrice() - prevCandle.getClosePrice()
            deltaHigh = candle.getHighPrice() - prevCandle.getHighPrice()
            deltaLow = candle.getLowPrice() - prevCandle.getLowPrice()

            scaledOpen = deltaOpen / self._maxAbsDelta[0] if self._maxAbsDelta[0] != 0 else 0
            scaledClose = deltaClose / self._maxAbsDelta[1] if self._maxAbsDelta[1] != 0 else 0
            scaledHigh = deltaHigh / self._maxAbsDelta[2] if self._maxAbsDelta[2] != 0 else 0
            scaledLow = deltaLow / self._maxAbsDelta[3] if self._maxAbsDelta[3] != 0 else 0

            normalized.append([scaledOpen, scaledClose, scaledHigh, scaledLow])

        return normalized
