from typing import List
from Interfaces import IPredictor, IReadonlyCandleSeries
from Entities import INextCandlePrediction, NextCandlePrediction, PredictionMeta
from ValueObjects import Candle

class CompositeAvgPredictor(IPredictor[INextCandlePrediction]):
    _predictors: List[IPredictor[INextCandlePrediction]]

    def __init__(self, predictors: List[IPredictor[INextCandlePrediction]]):
        if not predictors:
            raise ValueError("At least one predictor is required")
        self._predictors = predictors

    def predict(self, input: IReadonlyCandleSeries) -> INextCandlePrediction:
        predictions = [p.predict(input) for p in self._predictors]
        candles = [pred.getNextCandle() for pred in predictions]

        avgOpen = sum(c.getOpenPrice() for c in candles) / len(candles)
        avgClose = sum(c.getClosePrice() for c in candles) / len(candles)
        avgHigh = sum(c.getHighPrice() for c in candles) / len(candles)
        avgLow = sum(c.getLowPrice() for c in candles) / len(candles)
        avgVolume = sum(c.getVolume() for c in candles) / len(candles)

        highPrice = max(avgHigh, avgOpen, avgClose)
        lowPrice = min(avgLow, avgOpen, avgClose)

        firstCandle = candles[0]
        avgCandle = Candle(
            firstCandle.getAssetPair(),
            firstCandle.getOpenTimestamp(),
            firstCandle.getInterval(),
            avgOpen,
            avgClose,
            highPrice,
            lowPrice,
            avgVolume
        )

        meta = predictions[0].getMeta()
        return NextCandlePrediction(meta, avgCandle)

    def getCandlesCount(self) -> int:
        return max(p.getCandlesCount() for p in self._predictors)
