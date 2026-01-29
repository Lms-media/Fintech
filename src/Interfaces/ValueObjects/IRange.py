from abc import abstractmethod
from Primitives.IValueObject import IValueObject

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
    def includes(self, timestamp: int) -> bool:
        pass
