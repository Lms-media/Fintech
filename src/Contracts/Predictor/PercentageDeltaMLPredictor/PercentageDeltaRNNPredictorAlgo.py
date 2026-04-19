import numpy as np
from keras.models import Sequential
from keras.layers import Dense, Dropout, SimpleRNN
from keras.optimizers import Adam
from Interfaces import ICandleSeries
from .PercentageDeltaMLPredictorValue import PercentageDeltaMLPredictorValue
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo

class PercentageDeltaRNNPredictorAlgo(ITrainablePredictorAlgo[PercentageDeltaMLPredictorValue]):
    _candlesCount: int
    _maxOcDeltaAbs: float
    _maxOhDeltaAbs: float
    _maxOlDeltaAbs: float
    _maxOffsetAbs: float
    _normalizationInitialized: bool

    def __init__(self, candlesCount: int):
        self._candlesCount = candlesCount
        self._maxOhDeltaAbs = 0
        self._maxOcDeltaAbs = 0
        self._maxOlDeltaAbs = 0
        self._maxOffsetAbs = 0
        self._normalizationInitialized = False

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

        return PercentageDeltaMLPredictorValue(
            meta,
            result[0],
            self._maxOffsetAbs,
            result[1],
            self._maxOcDeltaAbs,
            result[2],
            self._maxOhDeltaAbs,
            result[3],
            self._maxOlDeltaAbs
        )

    def train(self, dataset: list[ICandleSeries], epochs: int = 100) -> None:
        if len(dataset) == 0:
            raise ValueError("Dataset is empty")

        if not self._normalizationInitialized:
            self._initNormalization(dataset)
            self._normalizationInitialized = True

        X_list = []
        y_list = []

        for series in dataset:
            normalized = self._normalize(series)
            X_list.append(normalized[:self._candlesCount])
            y_list.append(self._getTargetPct(series))

        X_train = np.array(X_list)
        y_train = np.array(y_list)

        self._model.fit(X_train, y_train, epochs=epochs, batch_size=32, validation_split=0.2)

    def _getTargetPct(self, series: ICandleSeries) -> list[float]:
        count = series.getCount()
        lastCandle = series.getByIndex(count - 1)
        prevCandle = series.getByIndex(count - 2)

        if not lastCandle or not prevCandle:
            return [0, 0, 0, 0]

        offset = lastCandle.getOpenPrice() - prevCandle.getClosePrice()
        ocDelta = lastCandle.getClosePrice() - lastCandle.getOpenPrice()
        ohDelta = lastCandle.getHighPrice() - lastCandle.getOpenPrice()
        olDelta = lastCandle.getLowPrice() - lastCandle.getOpenPrice()

        pctOffset = offset / prevCandle.getClosePrice()
        pctOcDelta = ocDelta / lastCandle.getOpenPrice()
        pctOhDelta = ohDelta / lastCandle.getOpenPrice()
        pctOlDelta = olDelta / lastCandle.getOpenPrice()

        scaledOffset = pctOffset / self._maxOffsetAbs
        scaledOcDelta = pctOcDelta / self._maxOcDeltaAbs
        scaledOhDelta = pctOhDelta / self._maxOhDeltaAbs
        scaledOlDelta = pctOlDelta / self._maxOlDeltaAbs

        return [scaledOffset, scaledOcDelta, scaledOhDelta, scaledOlDelta]

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
            count = item.getCount()

            for i in range(1, count):
                candle = item.getByIndex(i)
                prevCandle = item.getByIndex(i - 1)

                if not candle or not prevCandle:
                    continue

                offset = candle.getOpenPrice() - prevCandle.getClosePrice()
                ocDelta = candle.getClosePrice() - candle.getOpenPrice()
                ohDelta = candle.getHighPrice() - candle.getOpenPrice()
                olDelta = candle.getLowPrice() - candle.getOpenPrice()

                pctOffset = offset / prevCandle.getClosePrice()
                pctOcDelta = ocDelta / candle.getOpenPrice()
                pctOhDelta = ohDelta / candle.getOpenPrice()
                pctOlDelta = olDelta / candle.getOpenPrice()

                self._maxOffsetAbs = max(abs(pctOffset), self._maxOffsetAbs)
                self._maxOcDeltaAbs = max(abs(pctOcDelta), self._maxOcDeltaAbs)
                self._maxOhDeltaAbs = max(abs(pctOhDelta), self._maxOhDeltaAbs)
                self._maxOlDeltaAbs = max(abs(pctOlDelta), self._maxOlDeltaAbs)

    def _normalize(self, input: ICandleSeries) -> list[list[float]]:
        count = input.getCount()
        normalized = list[list[float]]()

        normalized.append([0, 0, 0, 0])

        for i in range(1, count):
            candle = input.getByIndex(i)
            prevCandle = input.getByIndex(i - 1)

            if not candle or not prevCandle:
                continue

            offset = candle.getOpenPrice() - prevCandle.getClosePrice()
            ocDelta = candle.getClosePrice() - candle.getOpenPrice()
            ohDelta = candle.getHighPrice() - candle.getOpenPrice()
            olDelta = candle.getLowPrice() - candle.getOpenPrice()

            pctOffset = offset / prevCandle.getClosePrice()
            pctOcDelta = ocDelta / candle.getOpenPrice()
            pctOhDelta = ohDelta / candle.getOpenPrice()
            pctOlDelta = olDelta / candle.getOpenPrice()

            scaledOffset = pctOffset / self._maxOffsetAbs
            scaledOcDelta = pctOcDelta / self._maxOcDeltaAbs
            scaledOhDelta = pctOhDelta / self._maxOhDeltaAbs
            scaledOlDelta = pctOlDelta / self._maxOlDeltaAbs

            normalized.append([scaledOffset, scaledOcDelta, scaledOhDelta, scaledOlDelta])

        return normalized
