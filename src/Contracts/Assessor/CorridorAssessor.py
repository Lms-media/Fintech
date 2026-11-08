from math import isfinite

from Interfaces import IAssessor
from Entities.CandleSeries.OpenCandleSeries import OpenCandleSeries
from Entities.Action.CandleCorridorAction import CandleCorridorAction
from ..Predictor.OpenClosePredictor import true_range


class CorridorAssessor(IAssessor):
    """Risk rules shared by algorithmic and ML forecast-driven strategies."""
    def __init__(self, portfolio, riskFraction=0.01, maxExposure=0.4,
                 atrPeriod=14, stopATR=1.0, profitATR=2.0,
                 minEdgeATR=0.1, minExpectedReturn=0.0, allowShort=True):
        parameters = [riskFraction, maxExposure, stopATR, profitATR, minEdgeATR, minExpectedReturn]
        if any(not isfinite(v) for v in parameters):
            raise ValueError("Assessor parameters must be finite")
        if not 0 < riskFraction <= 1 or not 0 < maxExposure <= 1:
            raise ValueError("Risk and exposure fractions must be in (0, 1]")
        if atrPeriod < 1 or stopATR <= 0 or profitATR <= 0 or minEdgeATR < 0 or minExpectedReturn < 0:
            raise ValueError("Invalid ATR or threshold parameters")
        self._portfolio = portfolio
        self.riskFraction, self.maxExposure = riskFraction, maxExposure
        self.atrPeriod, self.stopATR, self.profitATR = atrPeriod, stopATR, profitATR
        self.minEdgeATR, self.minExpectedReturn, self.allowShort = minEdgeATR, minExpectedReturn, allowShort

    def getAction(self, input):
        history = input.previousCandles
        if not isinstance(history, OpenCandleSeries):
            raise ValueError("CorridorAssessor requires closed history plus current open")
        current = history.getCurrentOpen()
        pair = current.assetPair
        if self._portfolio.getBaseAsset() != pair.getBaseAsset():
            raise ValueError("Portfolio and signal base assets must match")
        if input.getTimestamp() != current.timestamp:
            raise ValueError("Signal timestamp does not match current candle")
        if self._portfolio.getAssetAmount(pair.getQuoteAsset()) != 0:
            raise ValueError("Intracandle strategy requires a flat position before entry")
        price = current.price
        difference = input.getPrediction().getClosePrice() - price
        atr = true_range(history, self.atrPeriod)
        stop_distance, profit_distance = atr * self.stopATR, atr * self.profitATR
        first_buy = difference >= 0
        lower = price - (stop_distance if first_buy else profit_distance)
        upper = price + (profit_distance if first_buy else stop_distance)
        lots = 0
        cash = max(0, self._portfolio.getBaseAmount())
        lot_size = pair.getQuoteAsset().getLotSize()
        if (atr > 0 and lower > 0 and abs(difference) > max(atr * self.minEdgeATR, price * self.minExpectedReturn)
                and (first_buy or self.allowShort)):
            by_risk = cash * self.riskFraction / (stop_distance * lot_size)
            by_exposure = cash * self.maxExposure / (price * lot_size)
            lots = int(min(by_risk, by_exposure))
        # No market context is queried here: all decisions use opening-time data.
        return CandleCorridorAction(input, pair, lots, first_buy, lower, upper)
