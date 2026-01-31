from abc import abstractmethod
from src.Interfaces import IValueObject

class IRange(IValueObject['IRange']):

    @abstractmethod
    def getFromTimestamp(self) -> int:
        pass

    @abstractmethod
    def getToTimestamp(self) -> int:
        pass

    @abstractmethod
    def getDuration(self) -> int:
        pass

    @abstractmethod
    def withFromTimestamp(self, fromTimestamp: int) -> IRange:
        pass

    @abstractmethod
    def withToTimestamp(self, toTimestamp: int) -> IRange:
        pass

    @abstractmethod
    def includes(self, timestamp: int) -> bool:
        pass
