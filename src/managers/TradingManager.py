import abc
from src.strategies.Strategy import Strategy
from src.data.VirtualPortfolio import VirtualPortfolio

class TradingManager(abc.ABC):
    def __init__(self, instruments: dict[str, Strategy], chunkSize: int):
        self.instruments = instruments
        self.chunkSize = chunkSize
        self.virtualPortfolio: VirtualPortfolio = None
        
    def start(self):
        pass
    def stop(self):
        pass