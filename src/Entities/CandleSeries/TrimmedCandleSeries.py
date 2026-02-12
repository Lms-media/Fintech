from typing import Optional
import uuid
from Interfaces import IReadonlyCandleSeries, ICandle, IAssetPair, IRange

class TrimmedCandleSeries(IReadonlyCandleSeries):
    _id: str
    _base: IReadonlyCandleSeries
    _fromIndex: int
    _toIndex: int

    def __init__(self, base: IReadonlyCandleSeries, fromIndex: int, toIndex: int):
        self._id = uuid.uuid4()
        self._base = base
        self._fromIndex = fromIndex
        self._toIndex = toIndex

    def getId(self) -> str:
        return self._id

    def getCount(self) -> int:
        return self._toIndex - self._fromIndex

    def getByIndex(self, index: int) -> Optional[ICandle]:
        if 0 <= index < self._toIndex - self._fromIndex:
            return self._base.getByIndex(index - self._fromIndex)

        return None

    def getAssetPair(self) -> IAssetPair:
        return self._base.getAssetPair()

    def getByTimestamp(self, timestamp: int) -> Optional[ICandle]:
        interval = self._base.getByIndex(0).getInterval().value
        fromTimestamp = self._base.getByIndex(self._fromIndex).getOpenTimestamp()
        toTimestamp = self._base.getByIndex(self._toIndex).getOpenTimestamp() + interval
        if fromTimestamp <= timestamp < toTimestamp:
            return self._base.getByTimestamp(timestamp)

        return None

    def __str__(self) -> str:
        lines = list([f'📋 ({self.getId()})'])

        for i in range(self.getCount()):
            candle = self.getByIndex(i)
            lines.append(f"{i + 1}. {candle}")

        return '\n'.join(lines)
