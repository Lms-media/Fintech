"""Causal open-to-close predictors using the existing algo/adapter pipeline."""
from dataclasses import dataclass
from math import exp, isfinite, log, log1p

import numpy as np

from Interfaces import IPredictorAdapter, IPredictorAlgo
from Entities import PredictionMeta, NextCandlePrediction
from Entities.CandleSeries.OpenCandleSeries import OpenCandleSeries
from ValueObjects import Candle
from .APredictor import APredictor
from .ATrainablePredictor import ATrainablePredictor
from .Interfaces import ITrainablePredictorAlgo


def require_open(input, lookback):
    if not isinstance(input, OpenCandleSeries) or input.getCount() != lookback:
        raise ValueError(f"Expected OpenCandleSeries with {lookback} closed candles and a current open")
    return input.getCurrentOpen()


def true_range(history, period=14):
    ranges = []
    for i in range(max(0, history.getCount() - period), history.getCount()):
        candle = history.getByIndex(i)
        previous = history.getByIndex(i - 1)
        previous_close = previous.getClosePrice() if previous else candle.getOpenPrice()
        ranges.append(max(candle.getHighPrice() - candle.getLowPrice(),
                          abs(candle.getHighPrice() - previous_close),
                          abs(candle.getLowPrice() - previous_close)))
    return sum(ranges) / len(ranges) if ranges else 0.0


@dataclass(frozen=True)
class OpenCloseValue:
    meta: PredictionMeta
    closeReturn: float
    highReturn: float
    lowReturn: float


class OpenCloseAdapter(IPredictorAdapter):
    """Keep INextCandlePrediction for compatibility; its candle is current n."""
    def transform(self, value):
        current = value.meta.getCandleSeries().getCurrentOpen()
        close = current.price * exp(value.closeReturn)
        high = max(current.price, close, current.price * exp(value.highReturn))
        low = min(current.price, close, current.price * exp(value.lowReturn))
        candle = Candle(current.assetPair, current.timestamp, current.interval,
                        current.price, close, high, low, 0)
        return NextCandlePrediction(value.meta, candle)


class EWMACloseAlgo(IPredictorAlgo):
    def __init__(self, lookback, alpha=0.2, maxLogReturn=0.25):
        if lookback < 1 or not 0 < alpha <= 1 or not isfinite(maxLogReturn) or maxLogReturn <= 0:
            raise ValueError("Invalid EWMA parameters")
        self.lookback, self.alpha, self.maxLogReturn = lookback, alpha, maxLogReturn

    def calc(self, input):
        current = require_open(input, self.lookback)
        estimates = None
        for i in range(input.getCount()):
            c = input.getByIndex(i)
            values = [log(c.getClosePrice() / c.getOpenPrice()),
                      log(c.getHighPrice() / c.getOpenPrice()),
                      log(c.getLowPrice() / c.getOpenPrice())]
            estimates = values if estimates is None else [
                self.alpha * v + (1 - self.alpha) * old for v, old in zip(values, estimates)
            ]
        estimates = [max(-self.maxLogReturn, min(self.maxLogReturn, v)) for v in estimates]
        return OpenCloseValue(PredictionMeta(current.timestamp, input, 0.5), *estimates)


class EWMAClosePredictor(APredictor):
    def __init__(self, candlesCount=20, alpha=0.2, maxLogReturn=0.25):
        super().__init__(EWMACloseAlgo(candlesCount, alpha, maxLogReturn), OpenCloseAdapter(), candlesCount)

    def usesCurrentOpen(self):
        return True


class RidgeCloseAlgo(ITrainablePredictorAlgo):
    """Multioutput ridge regression; normalization is fitted on training only."""
    def __init__(self, lookback, regularization=1.0, maxLogReturn=0.25):
        if lookback < 1 or not isfinite(regularization) or regularization <= 0:
            raise ValueError("Positive lookback and regularization required")
        if not isfinite(maxLogReturn) or maxLogReturn <= 0:
            raise ValueError("Positive finite maxLogReturn required")
        self.lookback = lookback
        self.regularization = regularization
        self.maxLogReturn = maxLogReturn
        self._weights = None
        self._trainedThrough = None

    def features(self, input):
        current = require_open(input, self.lookback)
        values = []
        for i in range(input.getCount()):
            c = input.getByIndex(i)
            values.extend([log(c.getClosePrice() / c.getOpenPrice()),
                           log(c.getHighPrice() / c.getOpenPrice()),
                           log(c.getLowPrice() / c.getOpenPrice()),
                           log(c.getClosePrice() / current.price), log1p(c.getVolume())])
        values.append(log(current.price / input.getByIndex(input.getCount() - 1).getClosePrice()))
        return np.asarray(values, dtype=float)

    def train(self, dataset, epochs=100):
        if not dataset or epochs < 1:
            raise ValueError("Nonempty training dataset and positive epochs required")
        rows, targets, ends = [], [], []
        for series in dataset:
            if series.getCount() != self.lookback + 1:
                raise ValueError("Training sample must contain lookback closed candles and one target")
            target = series.getByIndex(self.lookback)
            # Also validate the complete label, but never include it in features.
            OpenCandleSeries._copy(target)
            opening = OpenCandleSeries.atIndex(series, self.lookback, self.lookback)
            rows.append(self.features(opening))
            targets.append([log(target.getClosePrice() / target.getOpenPrice()),
                            log(target.getHighPrice() / target.getOpenPrice()),
                            log(target.getLowPrice() / target.getOpenPrice())])
            ends.append(target.getOpenTimestamp() + target.getInterval().value)
        x, y = np.asarray(rows), np.asarray(targets)
        self._mean = x.mean(axis=0)
        scale = x.std(axis=0)
        self._scale = np.where(scale > 1e-12, scale, 1.0)
        design = np.column_stack([np.ones(len(x)), (x - self._mean) / self._scale])
        penalty = np.eye(design.shape[1]) * self.regularization ** 0.5
        penalty[0, 0] = 0  # Do not penalize intercept.
        self._weights = np.linalg.lstsq(
            np.vstack([design, penalty]),
            np.vstack([y, np.zeros((design.shape[1], 3))]), rcond=None
        )[0]
        self._trainedThrough = max(ends)

    def calc(self, input):
        current = require_open(input, self.lookback)
        if self._weights is None:
            raise ValueError("Train RidgeClosePredictor before prediction")
        if current.timestamp < self._trainedThrough:
            raise ValueError("Prediction overlaps training labels; use a chronological holdout")
        features = (self.features(input) - self._mean) / self._scale
        values = np.r_[1.0, features] @ self._weights
        values = np.clip(values, -self.maxLogReturn, self.maxLogReturn)
        return OpenCloseValue(PredictionMeta(current.timestamp, input, 0.5), *map(float, values))


class RidgeClosePredictor(ATrainablePredictor):
    def __init__(self, candlesCount=20, regularization=1.0, maxLogReturn=0.25):
        super().__init__(RidgeCloseAlgo(candlesCount, regularization, maxLogReturn),
                         OpenCloseAdapter(), candlesCount)

    def usesCurrentOpen(self):
        return True
