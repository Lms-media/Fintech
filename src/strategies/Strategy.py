import abc
from src.data.Candle import Candle

class Strategy(abc.ABC):
    def __init__(self, chunkSize: int):
        self.chunkSize = chunkSize
    
    def training(self, chunk: list[Candle], target: float):
        pass
    
    def predict(self, chunk: list[Candle]) -> float:
        pass