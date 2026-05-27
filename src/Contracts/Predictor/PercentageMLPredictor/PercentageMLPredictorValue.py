from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class PercentageMLPredictorValue():
    meta: IPredictionMeta
    outputs: list[float]
    maxAbsPct: list[float]
