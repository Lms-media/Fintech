import time
from typing import Optional
from Interfaces import (
    IExecutionContext,
    IContextProvider,
    IAssetPair,
    IDataSource,
    ICandle,
)
from ValueObjects import ExecutionContext


class DatasetContextProvider(IContextProvider):
    _startTimestamp: int
    _assetPair: IAssetPair

    def __init__(self, assetPair: IAssetPair, dataset: IDataSource):
        self._startTimestamp = int(time.time())
        self._assetPair = assetPair
        self._dataset = dataset

    def getContext(self, timestamp: int) -> IExecutionContext:
        openPrices = dict[IAssetPair, float]()
        closePrices = dict[IAssetPair, float]()
        hightPrices = dict[IAssetPair, float]()
        lowPrices = dict[IAssetPair, float]()
        candle = self._dataset.getSeries().getByTimestamp(timestamp)
        if candle:
            openPrices[self._assetPair] = candle.getOpenPrice()
            closePrices[self._assetPair] = candle.getClosePrice()
            hightPrices[self._assetPair] = candle.getHighPrice()
            lowPrices[self._assetPair] = candle.getLowPrice()
            return ExecutionContext(
                int(time.time()) - self._startTimestamp,
                openPrices,
                closePrices,
                hightPrices,
                lowPrices,
            )
        raise Exception("invalid candle exeption")

    def getNextCandle(self, candle: ICandle) -> Optional[ICandle]:
        candles = self._dataset.getSeries()
        nextCandle = candles.getByIndex(candles.getIndexOf(candle) + 1)
        return nextCandle
    
    def getPreviousCandle(self, timestamp: int):
        candles = self._dataset.getSeries()
        currentCandle = candles.getByTimestamp(timestamp)
        if currentCandle:
            previousCandle = candles.getByIndex(candles.getIndexOf(currentCandle) - 1)
            return previousCandle
        raise Exception("invalid candle exeption")
