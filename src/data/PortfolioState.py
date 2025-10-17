from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class PortfolioSate:
    amount: float
    assets: dict[str, float]
    datetime: datetime
    exchangeRates: dict[str, float]
    
    def getCapitalization(self) -> float:
        capital = 0
        for asset in self.assets:
            capital += self.assets[asset] * self.exchangeRates[asset]
            
        return self.amount + capital
        
