from src.strategies.Strategy import Strategy
from src.data.Candle import Candle
import pandas as pd
from dataclasses import asdict


class SimpleStrategy(Strategy):
    def __init__(self, chunkSize: int):
        self.chunkSize = chunkSize

    def predict(self, chunk: list[Candle]) -> float:
        data = [asdict(candle) for candle in chunk]
        df = pd.DataFrame(data)
        
        df['close_ma'] = df['close'].rolling(window=self.chunkSize).mean()
        
        if len(df) > 1 and df['close'].iloc[-1] > df['close'].iloc[-2]:
            return abs(df['close'].iloc[-1] - df['close'].iloc[-2])
        return -abs(df['close'].iloc[-1] - df['close'].iloc[-2])
