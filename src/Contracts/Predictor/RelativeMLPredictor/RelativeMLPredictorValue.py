from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class RelativeMLPredictorValue():
    meta: IPredictionMeta
    outputs: list[float]
    maxAbsDelta: list[float]
