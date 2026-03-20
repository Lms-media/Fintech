from typing import List
from Interfaces import IPredictor, IReadonlyCandleSeries
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle

class CompositeMedianPredictor(IPredictor[INextCandlePrediction]):
    _predictors: List[IPredictor[INextCandlePrediction]]

    def __init__(self, predictors: List[IPredictor[INextCandlePrediction]]):
        if not predictors:
            raise ValueError("At least one predictor is required")
        self._predictors = predictors

    def predict(self, input: IReadonlyCandleSeries) -> INextCandlePrediction:
        predictions = [p.predict(input) for p in self._predictors]
        candles = [pred.getNextCandle() for pred in predictions]

        opens = sorted([c.getOpenPrice() for c in candles])
        closes = sorted([c.getClosePrice() for c in candles])
        highs = sorted([c.getHighPrice() for c in candles])
        lows = sorted([c.getLowPrice() for c in candles])
        volumes = sorted([c.getVolume() for c in candles])

        medianOpen = self._median(opens)
        medianClose = self._median(closes)
        medianHigh = self._median(highs)
        medianLow = self._median(lows)
        medianVolume = self._median(volumes)

        highPrice = max(medianHigh, medianOpen, medianClose)
        lowPrice = min(medianLow, medianOpen, medianClose)

        firstCandle = candles[0]
        medianCandle = Candle(
            firstCandle.getAssetPair(),
            firstCandle.getOpenTimestamp(),
            firstCandle.getInterval(),
            medianOpen,
            medianClose,
            highPrice,
            lowPrice,
            medianVolume
        )

        meta = predictions[0].getMeta()
        return NextCandlePrediction(meta, medianCandle)

    def _median(self, values: List[float]) -> float:
        n = len(values)
        if n % 2 == 1:
            return values[n // 2]
        else:
            return (values[n // 2 - 1] + values[n // 2]) / 2

    def getCandlesCount(self) -> int:
        return max(p.getCandlesCount() for p in self._predictors)
