from abc import ABC, abstractmethod
from typing import TypeVar, Generic

T = TypeVar('T')

class IValueObject(ABC, Generic[T]):

    @abstractmethod
    def __eq__(self, other: T) -> bool:
        pass

    @abstractmethod
    def __hash__(self) -> int:
        pass

    @abstractmethod
    def __copy__(self) -> T:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass
