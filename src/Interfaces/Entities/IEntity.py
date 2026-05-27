from abc import ABC, abstractmethod

class IEntity(ABC):

    @abstractmethod
    def getId(self) -> str:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass
