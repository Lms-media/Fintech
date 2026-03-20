import numpy as np
from keras.models import Sequential
from keras.layers import Dense, Dropout, SimpleRNN
from keras.optimizers import Adam
from Interfaces import ICandleSeries
from .PercentageMLPredictorValue import PercentageMLPredictorValue
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo

class PercentageRNNPredictorAlgo(ITrainablePredictorAlgo[PercentageMLPredictorValue]):
    _candlesCount: int
    _maxAbsPct: list[float]

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount
        self._maxAbsPct = [0.0, 0.0, 0.0, 0.0]

        self._model = Sequential([
            SimpleRNN(50, return_sequences=False, input_shape=(candlesCount, 4)),
            Dense(25, activation='relu'),
            Dropout(0.2),
            Dense(4, activation='linear')
            # SimpleRNN(50, return_sequences=True, input_shape=(candlesCount, 4)),
            # SimpleRNN(30, return_sequences=False),
            # Dense(25, activation='relu'),
            # Dropout(0.2),
            # Dense(4, activation='linear')
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

        limits = [
            (-self._maxAbsPct[0], self._maxAbsPct[0]),
            (-self._maxAbsPct[1], self._maxAbsPct[1]),
            (-self._maxAbsPct[2], self._maxAbsPct[2]),
            (-self._maxAbsPct[3], self._maxAbsPct[3]),
        ]

        return PercentageMLPredictorValue(meta, result, limits)

    def train(self, dataset: list[ICandleSeries]) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        self._initNormalization(dataset)

        X_list = []
        y_list = []

        for series in dataset:
            normalized = self._normalize(series)
            X_list.append(normalized[:self._candlesCount])
            y_list.append(self._getTargetPct(series))

        X_train = np.array(X_list)
        y_train = np.array(y_list)

        self._model.fit(X_train, y_train, epochs=100, batch_size=32, validation_split=0.2, verbose='auto')

    def _getTargetPct(self, series: ICandleSeries) -> list[float]:
        count = series.getCount()
        lastCandle = series.getByIndex(count - 1)
        prevCandle = series.getByIndex(count - 2)

        if not lastCandle or not prevCandle:
            return [0, 0, 0, 0]

        pctOpen = (lastCandle.getOpenPrice() - prevCandle.getOpenPrice()) / prevCandle.getOpenPrice()
        pctClose = (lastCandle.getClosePrice() - prevCandle.getClosePrice()) / prevCandle.getClosePrice()
        pctHigh = (lastCandle.getHighPrice() - prevCandle.getHighPrice()) / prevCandle.getHighPrice()
        pctLow = (lastCandle.getLowPrice() - prevCandle.getLowPrice()) / prevCandle.getLowPrice()

        scaledOpen = pctOpen / self._maxAbsPct[0] if self._maxAbsPct[0] != 0 else 0
        scaledClose = pctClose / self._maxAbsPct[1] if self._maxAbsPct[1] != 0 else 0
        scaledHigh = pctHigh / self._maxAbsPct[2] if self._maxAbsPct[2] != 0 else 0
        scaledLow = pctLow / self._maxAbsPct[3] if self._maxAbsPct[3] != 0 else 0

        return [scaledOpen, scaledClose, scaledHigh, scaledLow]

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
            count = item.getCount()
            firstCandle = item.getByIndex(0)

            if not firstCandle:
                continue

            baseOpen = firstCandle.getOpenPrice()
            baseClose = firstCandle.getClosePrice()
            baseHigh = firstCandle.getHighPrice()
            baseLow = firstCandle.getLowPrice()

            for i in range(1, count):
                candle = item.getByIndex(i)
                prevCandle = item.getByIndex(i - 1)

                if not candle or not prevCandle:
                    continue

                pctOpenCum = (candle.getOpenPrice() - baseOpen) / baseOpen
                pctCloseCum = (candle.getClosePrice() - baseClose) / baseClose
                pctHighCum = (candle.getHighPrice() - baseHigh) / baseHigh
                pctLowCum = (candle.getLowPrice() - baseLow) / baseLow

                pctOpenInc = (candle.getOpenPrice() - prevCandle.getOpenPrice()) / prevCandle.getOpenPrice()
                pctCloseInc = (candle.getClosePrice() - prevCandle.getClosePrice()) / prevCandle.getClosePrice()
                pctHighInc = (candle.getHighPrice() - prevCandle.getHighPrice()) / prevCandle.getHighPrice()
                pctLowInc = (candle.getLowPrice() - prevCandle.getLowPrice()) / prevCandle.getLowPrice()

                self._maxAbsPct[0] = max(abs(pctOpenCum), abs(pctOpenInc), self._maxAbsPct[0])
                self._maxAbsPct[1] = max(abs(pctCloseCum), abs(pctCloseInc), self._maxAbsPct[1])
                self._maxAbsPct[2] = max(abs(pctHighCum), abs(pctHighInc), self._maxAbsPct[2])
                self._maxAbsPct[3] = max(abs(pctLowCum), abs(pctLowInc), self._maxAbsPct[3])

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

        normalized.append([0, 0, 0, 0])

        for i in range(1, count):
            candle = input.getByIndex(i)

            if not candle:
                normalized.append([0, 0, 0, 0])
                continue

            pctOpen = (candle.getOpenPrice() - baseOpen) / baseOpen
            pctClose = (candle.getClosePrice() - baseClose) / baseClose
            pctHigh = (candle.getHighPrice() - baseHigh) / baseHigh
            pctLow = (candle.getLowPrice() - baseLow) / baseLow

            scaledOpen = pctOpen / self._maxAbsPct[0] if self._maxAbsPct[0] != 0 else 0
            scaledClose = pctClose / self._maxAbsPct[1] if self._maxAbsPct[1] != 0 else 0
            scaledHigh = pctHigh / self._maxAbsPct[2] if self._maxAbsPct[2] != 0 else 0
            scaledLow = pctLow / self._maxAbsPct[3] if self._maxAbsPct[3] != 0 else 0

            normalized.append([scaledOpen, scaledClose, scaledHigh, scaledLow])

        return normalized
