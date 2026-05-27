from abc import ABC, abstractmethod

class ILogger(ABC):

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def log(self, chunk: str):
        pass
