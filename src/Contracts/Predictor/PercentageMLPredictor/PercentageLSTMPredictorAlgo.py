import numpy as np
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM
from keras.optimizers import Adam
from Interfaces import ICandleSeries
from .PercentageMLPredictorValue import PercentageMLPredictorValue
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo

class PercentageLSTMPredictorAlgo(ITrainablePredictorAlgo[PercentageMLPredictorValue]):
    _candlesCount: int
    _pctLimits: list[tuple[float, float]]

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount

        self._model = Sequential([
            # LSTM(50, return_sequences=False, input_shape=(candlesCount, 4)),
            # Dense(25, activation='relu'),
            # Dropout(0.2),
            # Dense(4, activation='linear')
            LSTM(50, return_sequences=True, input_shape=(candlesCount, 4)),
            LSTM(30, return_sequences=False),
            Dense(25, activation='relu'),
            Dropout(0.2),
            Dense(4, activation='linear')
        ])
        self._model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )

        self._pctLimits = [
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

        return PercentageMLPredictorValue(meta, result, self._pctLimits)

    def train(self, dataset: list[ICandleSeries], epochs: int = 100) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        self._initNormalization(dataset)

        X_list = []
        y_list = []

        for series in dataset:
            normalized = self._normalize(series)
            pctNormalized = self._normalizePercentage(series)
            X_list.append(normalized[:self._candlesCount])
            y_list.append(pctNormalized)

        X_train = np.array(X_list)
        y_train = np.array(y_list)

        self._model.fit(X_train, y_train, epochs=epochs, batch_size=32, validation_split=0.2, verbose='auto')

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
            count = item.getCount()
            lastCandle = item.getByIndex(count - 1)
            preLastCandle = item.getByIndex(count - 2)

            if not lastCandle or not preLastCandle:
                raise ValueError("Dataset parsing error")

            pctOpen = (lastCandle.getOpenPrice() - preLastCandle.getOpenPrice()) / preLastCandle.getOpenPrice()
            pctClose = (lastCandle.getClosePrice() - preLastCandle.getClosePrice()) / preLastCandle.getClosePrice()
            pctHigh = (lastCandle.getHighPrice() - preLastCandle.getHighPrice()) / preLastCandle.getHighPrice()
            pctLow = (lastCandle.getLowPrice() - preLastCandle.getLowPrice()) / preLastCandle.getLowPrice()

            self._pctLimits[0] = (min(pctOpen, self._pctLimits[0][0]), max(pctOpen, self._pctLimits[0][1]))
            self._pctLimits[1] = (min(pctClose, self._pctLimits[1][0]), max(pctClose, self._pctLimits[1][1]))
            self._pctLimits[2] = (min(pctHigh, self._pctLimits[2][0]), max(pctHigh, self._pctLimits[2][1]))
            self._pctLimits[3] = (min(pctLow, self._pctLimits[3][0]), max(pctLow, self._pctLimits[3][1]))

    def _normalize(self, input: ICandleSeries) -> list[list[float]]:
        count = input.getCount()
        normalized = list[list[float]]()

        firstCandle = input.getByIndex(0)
        if not firstCandle:
            raise ValueError("Cannot get first candle")

        baseOpen = firstCandle.getOpenPrice()
        baseClose = firstCandle.getClosePrice()
        baseHigh = firstCandle.getHighPrice()
        baseLow = firstCandle.getLowPrice()

        for i in range(count):
            candle = input.getByIndex(i)

            if not candle:
                normalized.append([0, 0, 0, 0])
                continue

            pctOpen = (candle.getOpenPrice() - baseOpen) / baseOpen
            pctClose = (candle.getClosePrice() - baseClose) / baseClose
            pctHigh = (candle.getHighPrice() - baseHigh) / baseHigh
            pctLow = (candle.getLowPrice() - baseLow) / baseLow

            normalized.append([pctOpen, pctClose, pctHigh, pctLow])

        return normalized

    def _normalizePercentage(self, input: ICandleSeries) -> list[float]:
        count = input.getCount()

        lastCandle = input.getByIndex(count - 1)
        preLastCandle = input.getByIndex(count - 2)

        if not lastCandle or not preLastCandle:
            raise ValueError("Percentage normalization failed")

        pctOpen = (lastCandle.getOpenPrice() - preLastCandle.getOpenPrice()) / preLastCandle.getOpenPrice()
        pctClose = (lastCandle.getClosePrice() - preLastCandle.getClosePrice()) / preLastCandle.getClosePrice()
        pctHigh = (lastCandle.getHighPrice() - preLastCandle.getHighPrice()) / preLastCandle.getHighPrice()
        pctLow = (lastCandle.getLowPrice() - preLastCandle.getLowPrice()) / preLastCandle.getLowPrice()

        scaledPctOpen = (pctOpen - self._pctLimits[0][0]) / (self._pctLimits[0][1] - self._pctLimits[0][0])
        scaledPctClose = (pctClose - self._pctLimits[1][0]) / (self._pctLimits[1][1] - self._pctLimits[1][0])
        scaledPctHigh = (pctHigh - self._pctLimits[2][0]) / (self._pctLimits[2][1] - self._pctLimits[2][0])
        scaledPctLow = (pctLow - self._pctLimits[3][0]) / (self._pctLimits[3][1] - self._pctLimits[3][0])

        return [scaledPctOpen, scaledPctClose, scaledPctHigh, scaledPctLow]
