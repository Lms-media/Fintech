from src.Interfaces import IRange

class Range(IRange):
    _fromTimestamp: int
    _toTimestamp: int

    def __init__(self, fromTimestamp: int, toTimestamp: int):
        if fromTimestamp > toTimestamp:
            raise ValueError(f"'fromTimestamp' must be greater than 'toTimestamp', but 'fromTimestamp' is {fromTimestamp} and 'toTimestamp' is {toTimestamp}")

        if fromTimestamp < 0:
            raise ValueError(f"'fromTimestamp' must be greater than or equal to zero, but 'fromTimestamp' is {fromTimestamp}")

        if toTimestamp < 0:
            raise ValueError(f"'toTimestamp' must be greater than or equal to zero, but 'toTimestamp' is {toTimestamp}")

        self._fromTimestamp = fromTimestamp
        self._toTimestamp = toTimestamp

    def getFromTimestamp(self) -> int:
        return self._fromTimestamp

    def getToTimestamp(self) -> int:
        return self._toTimestamp

    def getDuration(self) -> int:
        return self._toTimestamp - self._fromTimestamp

    def withFromTimestamp(self, fromTimestamp: int) -> IRange:
        if fromTimestamp > self._toTimestamp:
            raise ValueError(f"'fromTimestamp' must be greater than 'toTimestamp', but 'fromTimestamp' is {fromTimestamp} and 'toTimestamp' is {self._toTimestamp}")

        if fromTimestamp < 0:
            raise ValueError(f"'fromTimestamp' must be greater than or equal to zero, but 'fromTimestamp' is {fromTimestamp}")

        return Range(fromTimestamp, self._toTimestamp)

    def withToTimestamp(self, toTimestamp: int) -> IRange:
        if self._fromTimestamp > toTimestamp:
            raise ValueError(f"'fromTimestamp' must be greater than 'toTimestamp', but 'fromTimestamp' is {self._fromTimestamp} and 'toTimestamp' is {toTimestamp}")

        if toTimestamp < 0:
            raise ValueError(f"'toTimestamp' must be greater than or equal to zero, but 'toTimestamp' is {toTimestamp}")

        return Range(self._fromTimestamp, toTimestamp)

    def includes(self, timestamp: int) -> bool:
        return self._fromTimestamp <= timestamp <= self._toTimestamp

    def __eq__(self, other: IRange) -> bool:
        if not isinstance(other, Range):
            return False
        return (self._fromTimestamp == other.getFromTimestamp() and
                self._toTimestamp == other.getToTimestamp())

    def __hash__(self) -> int:
        return hash((self._fromTimestamp, self._toTimestamp))

    def __copy__(self) -> IRange:
        return Range(self._fromTimestamp, self._toTimestamp)

    def __str__(self) -> str:
        return f"[{self._fromTimestamp}, {self._toTimestamp}]"
