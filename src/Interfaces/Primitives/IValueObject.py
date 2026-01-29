from abc import ABC, abstractmethod
from typing import TypeVar, Generic

T = TypeVar('T')

class IValueObject(ABC, Generic[T]):

    @abstractmethod
    def equals(self, other: T) -> bool:
        pass

    @abstractmethod
    def copy(self) -> T:
        pass
