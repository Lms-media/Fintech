from src.Interfaces import IAsset

class Asset(IAsset):

    def __init__(self, tickerCode: str, lotSize: int):
        if tickerCode == '':
            raise ValueError(f"'tickerCode' must be a non-empty string")

        if lotSize <= 0:
            raise ValueError(f"'lotSize' must be greater than zero")

        self._tickerCode = tickerCode
        self._lotSize = lotSize

    def getTickerCode(self) -> str:
        return self._tickerCode

    def getLotSize(self) -> int:
        return self._lotSize

    def withTickerCode(self, tickerCode) -> IAsset:
        return Asset(tickerCode, self._lotSize)

    def withLotSize(self, lotSize) -> IAsset:
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
