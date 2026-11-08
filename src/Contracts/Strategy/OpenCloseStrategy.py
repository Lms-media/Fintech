from math import isclose

from Interfaces import IStrategy
from Entities import CandleSignal
from Entities.CandleSeries.OpenCandleSeries import OpenCandleSeries


class OpenCloseStrategy(IStrategy):
    """Bridge current-candle forecasts to the existing assessor interface."""
    def getSignal(self, input):
        history = input.getMeta().getCandleSeries()
        if not isinstance(history, OpenCandleSeries):
            raise ValueError("OpenCloseStrategy requires an opening-time prediction")
        current = history.getCurrentOpen()
        candle = input.getNextCandle()
        if (candle.getOpenTimestamp() != current.timestamp
                or candle.getAssetPair() != current.assetPair
                or candle.getInterval() != current.interval
                or not isclose(candle.getOpenPrice(), current.price, rel_tol=1e-12)):
            raise ValueError("Forecast must refer to the known opening of the current candle")
        return CandleSignal(current.timestamp, candle, history)
