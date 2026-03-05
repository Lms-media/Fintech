from typing import Optional
import uuid
from Interfaces import IReadonlyCandleSeries, ICandle, IAssetPair, IRange

class TrimmedCandleSeries(IReadonlyCandleSeries):
    _id: str
    _base: IReadonlyCandleSeries
    _fromIndex: int
    _toIndex: int

    def __init__(self, base: IReadonlyCandleSeries, fromIndex: int, toIndex: int):
        self._id = str(uuid.uuid4())
        self._base = base
        self._fromIndex = fromIndex
        self._toIndex = toIndex

    def getId(self) -> str:
        return self._id

    def getCount(self) -> int:
        return self._toIndex - self._fromIndex

    def getByIndex(self, index: int) -> Optional[ICandle]:
        if 0 <= index < self._toIndex - self._fromIndex:
            return self._base.getByIndex(index + self._fromIndex)

        return None
    
    def getIndexOf(self, candle: ICandle) -> int:
        baseIndex = self._base.getIndexOf(candle)
        return baseIndex - self._fromIndex if baseIndex >= self._fromIndex else -1

    def getAssetPair(self) -> IAssetPair:
        return self._base.getAssetPair()

    def getByTimestamp(self, timestamp: int) -> Optional[ICandle]:
        fromCandle = self._base.getByIndex(self._fromIndex)
        toCandle = self._base.getByIndex(self._toIndex)

        if not fromCandle or not toCandle:
            return None

        interval = fromCandle.getInterval().value
        fromTimestamp = fromCandle.getOpenTimestamp()
        toTimestamp = toCandle.getOpenTimestamp() + interval
        if fromTimestamp <= timestamp < toTimestamp:
            return self._base.getByTimestamp(timestamp)

        return None

    def __str__(self) -> str:
        lines = list([f'📋 ({self.getId()})'])

        for i in range(self.getCount()):
            candle = self.getByIndex(i)
            lines.append(f"{i + 1}. {candle}")

        return '\n'.join(lines)
