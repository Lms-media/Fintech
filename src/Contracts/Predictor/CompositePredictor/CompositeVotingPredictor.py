from typing import List
from Interfaces import IPredictor, IReadonlyCandleSeries
from Entities import INextCandlePrediction, NextCandlePrediction, PredictionMeta
from ValueObjects import Candle

class CompositeVotingPredictor(IPredictor[INextCandlePrediction]):
    _predictors: List[IPredictor[INextCandlePrediction]]

    def __init__(self, predictors: List[IPredictor[INextCandlePrediction]]):
        if not predictors:
            raise ValueError("At least one predictor is required")
        self._predictors = predictors

    def predict(self, input: IReadonlyCandleSeries) -> INextCandlePrediction:
        predictions = [p.predict(input) for p in self._predictors]
        candles = [pred.getNextCandle() for pred in predictions]

        lastCandle = input.getByIndex(input.getCount() - 1)
        if not lastCandle:
            raise ValueError("Cannot extract last candle from input")

        lastClose = lastCandle.getClosePrice()

        upVotes = 0
        downVotes = 0
        upDeltas = []
        downDeltas = []

        for candle in candles:
            predictedClose = candle.getClosePrice()
            delta = predictedClose - lastClose
            if delta > 0:
                upVotes += 1
                upDeltas.append(delta)
            elif delta < 0:
                downVotes += 1
                downDeltas.append(delta)

        if upVotes > downVotes:
            avgDelta = sum(upDeltas) / len(upDeltas) if upDeltas else 0
        elif downVotes > upVotes:
            avgDelta = sum(downDeltas) / len(downDeltas) if downDeltas else 0
        else:
            avgDelta = 0

        firstCandle = candles[0]
        openPrice = lastClose
        closePrice = lastClose + avgDelta
        highPrice = max(openPrice, closePrice)
        lowPrice = min(openPrice, closePrice)

        votingCandle = Candle(
            firstCandle.getAssetPair(),
            firstCandle.getOpenTimestamp(),
            firstCandle.getInterval(),
            openPrice,
            closePrice,
            highPrice,
            lowPrice,
            firstCandle.getVolume()
        )

        meta = predictions[0].getMeta()
        return NextCandlePrediction(meta, votingCandle)

    def getCandlesCount(self) -> int:
        return max(p.getCandlesCount() for p in self._predictors)
