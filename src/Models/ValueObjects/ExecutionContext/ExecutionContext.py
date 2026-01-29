from src.Interfaces import IRange

class Range(IRange):

    def __init__(self, from_timestamp: int, to_timestamp: int):
        if from_timestamp > to_timestamp:
            raise ValueError(f"from_timestamp must be greater than to_timestamp")

        self._from_timestamp = from_timestamp
        self._to_timestamp = to_timestamp

    def getFromTimestamp(self) -> int:
        return self._from_timestamp

    def getToTimestamp(self) -> int:
        return self._to_timestamp

    def getDuration(self) -> int:
        return self._to_timestamp - self._from_timestamp

    def includes(self, timestamp: int) -> bool:
        return self._from_timestamp <= timestamp <= self._to_timestamp

    def equals(self, other: IRange) -> bool:
        if not isinstance(other, Range):
            return False
        return (self._from_timestamp == other.getFromTimestamp() and
                self._to_timestamp == other.getToTimestamp())

    def copy(self) -> IRange:
        return Range(self._from_timestamp, self._to_timestamp)

    def __str__(self) -> str:
        return f"[{self._from_timestamp}, {self._to_timestamp}]"
