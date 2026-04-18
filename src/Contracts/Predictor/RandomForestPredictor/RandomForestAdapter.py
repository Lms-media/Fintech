from typing import List, Tuple
from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from .RandomForestPredictorValue import RandomForestPredictorValue


class RandomForestPredictorAdapter(IPredictorAdapter[RandomForestPredictorValue, INextCandlePrediction]):

    def transform(self, value: RandomForestPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        outputs: List[float] = list(value.outputs) if value.outputs is not None else []
        limits: List[Tuple[float, float]] = list(value.limits) if value.limits is not None else [(0.0, 1.0)] * 4

        # basic validation
        if len(outputs) < 4:
            raise ValueError("RandomForestPredictorAdapter: expected 4 outputs (open, close, high, low)")
        if len(limits) < 4:
            # fill missing limits with neutral defaults
            limits = (limits + [(0.0, 1.0)] * 4)[:4]

        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)

        if not lastCandle:
            raise ValueError(f"Failed to parse last candle from meta")

        interval = lastCandle.getInterval()
        # prefer timestamp from meta if provided
        timestamp = getattr(meta, 'getTimestamp', lambda: None)()
        if timestamp is None:
            # fall back to last candle timestamp + interval
            try:
                timestamp = lastCandle.getOpenTimestamp() + interval.value
            except Exception:
                timestamp = lastCandle.getOpenTimestamp()
        volume = 1

        # RandomForestAlgo predicts absolute OHLC prices, not normalized values.
        try:
            r_open, r_close, r_high, r_low = [float(x) for x in outputs[:4]]

            openPrice = lastCandle.getOpenPrice() * (1.0 + r_open)
            closePrice = lastCandle.getClosePrice() * (1.0 + r_close)
            highPrice = lastCandle.getHighPrice() * (1.0 + r_high)
            lowPrice = lastCandle.getLowPrice() * (1.0 + r_low)
        except Exception:
            raise ValueError("RandomForestPredictorAdapter: failed to parse outputs into prices")

        highPrice = max(openPrice, closePrice, highPrice, lowPrice)
        lowPrice = min(openPrice, closePrice, highPrice, lowPrice)

        from ValueObjects import Candle
        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
