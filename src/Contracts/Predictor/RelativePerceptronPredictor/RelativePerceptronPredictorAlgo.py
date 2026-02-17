import numpy as np
from keras.models import Sequential
from keras.layers import Dense, Dropout, Input, Flatten
from keras.optimizers import Adam
from Interfaces import ICandleSeries
from .RelativePerceptronPredictorValue import RelativePerceptronPredictorValue
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo

class RelativePerceptronPredictorAlgo(ITrainablePredictorAlgo[RelativePerceptronPredictorValue]):
    _candlesCount: int
    _limits: list[tuple[float, float]]
    _deltaLimits: list[tuple[float, float]]

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount

        self._model = Sequential([
            Input(shape=(candlesCount, 4), name='input'),
            Flatten(name='flatten'),
            Dense(64, activation='relu', name='hidden_1'),
            Dropout(0.2, name='dropout_1'),
            Dense(32, activation='relu', name='hidden_2'),
            Dense(4, name='output')
        ])
        self._model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )

        self._limits = self._deltaLimits = [
            (float('inf'), float('-inf')),
            (float('inf'), float('-inf')),
            (float('inf'), float('-inf')),
            (float('inf'), float('-inf')),
        ]

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

        return RelativePerceptronPredictorValue(meta, result, self._deltaLimits)

    def train(self, dataset: list[ICandleSeries]) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        self._initNormalization(dataset)

        X_list = []
        y_list = []

        for series in dataset:
            normalized = self._normalize(series)
            deltaNormalized = self._normalizeDelta(series)
            X_list.append(normalized[:self._candlesCount])
            y_list.append(deltaNormalized)

        X_train = np.array(X_list)
        y_train = np.array(y_list)

        self._model.fit(X_train, y_train, epochs=100, batch_size=32, validation_split=0.2, verbose='auto')

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
            count = item.getCount()
            lastCandle = item.getByIndex(count - 1)
            preLastCandle = item.getByIndex(count - 2)

            if not lastCandle or not preLastCandle:
                raise ValueError("Dataset persing error")

            deltaOpenPrice = lastCandle.getOpenPrice() - preLastCandle.getOpenPrice()
            deltaClosePrice = lastCandle.getClosePrice() - preLastCandle.getClosePrice()
            deltaHighPrice = lastCandle.getHighPrice() - preLastCandle.getHighPrice()
            deltaLowPrice = lastCandle.getLowPrice() - preLastCandle.getLowPrice()

            self._deltaLimits[0] = (min(deltaOpenPrice, self._limits[0][0]), max(deltaOpenPrice, self._limits[0][1]))
            self._deltaLimits[1] = (min(deltaClosePrice, self._limits[1][0]), max(deltaClosePrice, self._limits[1][1]))
            self._deltaLimits[2] = (min(deltaHighPrice, self._limits[2][0]), max(deltaHighPrice, self._limits[2][1]))
            self._deltaLimits[3] = (min(deltaOpenPrice, self._limits[3][0]), max(deltaLowPrice, self._limits[3][1]))

            for i in range(item.getCount()):
                candle = item.getByIndex(i)

                if not candle:
                    raise ValueError("Dataset parsing error")

                self._limits[0] = (min(candle.getOpenPrice(), self._limits[0][0]), max(candle.getOpenPrice(), self._limits[0][1]))
                self._limits[1] = (min(candle.getClosePrice(), self._limits[1][0]), max(candle.getClosePrice(), self._limits[1][1]))
                self._limits[2] = (min(candle.getHighPrice(), self._limits[2][0]), max(candle.getHighPrice(), self._limits[2][1]))
                self._limits[3] = (min(candle.getLowPrice(), self._limits[3][0]), max(candle.getLowPrice(), self._limits[3][1]))

    def _normalize(self, input: ICandleSeries) -> list[list[float]]:
        count = input.getCount()
        normalized = list[list[float]]()

        for i in range(count):
            candle = input.getByIndex(i)

            if not candle:
                normalized.append([0, 0, 0, 0])
                continue

            scaledOpen = (candle.getOpenPrice() - self._limits[0][0]) / (self._limits[0][1] - self._limits[0][0])
            scaledClose = (candle.getClosePrice() - self._limits[1][0]) / (self._limits[1][1] - self._limits[1][0])
            scaledHigh = (candle.getHighPrice() - self._limits[2][0]) / (self._limits[2][1] - self._limits[2][0])
            scaledLow = (candle.getLowPrice() - self._limits[3][0]) / (self._limits[3][1] - self._limits[3][0])

            normalized.append([scaledOpen, scaledClose, scaledHigh, scaledLow])

        return normalized

    def _normalizeDelta(self, input: ICandleSeries) -> list[float]:
        count = input.getCount()

        lastCandle = input.getByIndex(count - 1)
        preLastCandle = input.getByIndex(count - 2)

        if not lastCandle or not preLastCandle:
            raise ValueError("Delta normalization failed")

        deltaOpenPrice = lastCandle.getOpenPrice() - preLastCandle.getOpenPrice()
        deltaClosePrice = lastCandle.getClosePrice() - preLastCandle.getClosePrice()
        deltaHighPrice = lastCandle.getHighPrice() - preLastCandle.getHighPrice()
        deltaLowPrice = lastCandle.getLowPrice() - preLastCandle.getLowPrice()

        scaledDeltaOpen = (deltaOpenPrice - self._deltaLimits[0][0]) / (self._deltaLimits[0][1] - self._deltaLimits[0][0])
        scaledDeltaClose = (deltaClosePrice - self._deltaLimits[1][0]) / (self._deltaLimits[1][1] - self._deltaLimits[1][0])
        scaledDeltaHigh = (deltaHighPrice - self._deltaLimits[2][0]) / (self._deltaLimits[2][1] - self._deltaLimits[2][0])
        scaledDeltaLow = (deltaLowPrice - self._deltaLimits[3][0]) / (self._deltaLimits[3][1] - self._deltaLimits[3][0])

        return [scaledDeltaOpen, scaledDeltaClose, scaledDeltaHigh, scaledDeltaLow]
