from abc import ABC, abstractmethod

class IUseCase(ABC):

    @abstractmethod
    def execute(self) -> None:
        pass
