from __future__ import annotations
from Interfaces import IAsset

class Asset(IAsset):
    _tickerCode: str
    _lotSize: int

    def __init__(self, tickerCode: str, lotSize: int):
        if tickerCode == '':
            raise ValueError(f"'tickerCode' must be a non-empty string, but 'tickerCode' is {tickerCode}")

        if lotSize <= 0:
            raise ValueError(f"'lotSize' must be greater than zero, but 'lotSize' is {lotSize}")

        self._tickerCode = tickerCode
        self._lotSize = lotSize

    def getTickerCode(self) -> str:
        return self._tickerCode

    def getLotSize(self) -> int:
        return self._lotSize

    def withTickerCode(self, tickerCode) -> IAsset:
        if tickerCode == '':
            raise ValueError(f"'tickerCode' must be a non-empty string, but 'tickerCode' is {tickerCode}")
        return Asset(tickerCode, self._lotSize)

    def withLotSize(self, lotSize) -> IAsset:
        if lotSize <= 0:
            raise ValueError(f"'lotSize' must be greater than zero, but 'lotSize' is: {lotSize}")
        return Asset(self._tickerCode, lotSize)

    def __eq__(self, other: IAsset) -> bool:
        if not isinstance(other, Asset):
            return False
        return (self._tickerCode == other.getTickerCode() and
                self._lotSize == other.getLotSize())

    def __hash__(self) -> int:
        return hash((self._tickerCode, self._lotSize))

    def __copy__(self) -> IAsset:
        return Asset(self._tickerCode, self._lotSize)

    def __str__(self) -> str:
        return f"💵 {self._tickerCode} ({self._lotSize})"
