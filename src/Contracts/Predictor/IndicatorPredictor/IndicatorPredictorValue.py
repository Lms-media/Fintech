from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class IndicatorPredictorValue():
    meta: IPredictionMeta
    priceDelta: float
