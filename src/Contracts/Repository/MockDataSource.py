from ValueObjects import Candle
from Interfaces import IDataSource, ICandleSeries, IAssetPair, IntervalType

class MockDataSource(IDataSource):
    _assetPair: IAssetPair
    _series: ICandleSeries

    def __init__(self, assetPair: IAssetPair):
        self._assetPair = assetPair

    def init(self) -> None:
        self._series.appendRight(Candle(self._assetPair, 0, IntervalType.OneMinute, 1, 2, 1, 3, 1))
        self._series.appendRight(Candle(self._assetPair, 60, IntervalType.OneMinute, 2, 4, 1.5, 5, 2))
        self._series.appendRight(Candle(self._assetPair, 120, IntervalType.OneMinute, 4, 6, 3, 8, 2))
        self._series.appendRight(Candle(self._assetPair, 180, IntervalType.OneMinute, 6, 6, 5, 7, 1))
        self._series.appendRight(Candle(self._assetPair, 240, IntervalType.OneMinute, 6, 7, 6, 7, 1))
        self._series.appendRight(Candle(self._assetPair, 300, IntervalType.OneMinute, 7, 4, 7, 4, 2))
        self._series.appendRight(Candle(self._assetPair, 360, IntervalType.OneMinute, 4, 3, 4, 1, 2))
        self._series.appendRight(Candle(self._assetPair, 420, IntervalType.OneMinute, 3, 1, 3, 1, 1))

    def getSeries(self):
        return self._series
