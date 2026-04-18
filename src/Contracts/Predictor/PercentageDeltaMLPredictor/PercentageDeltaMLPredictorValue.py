from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class PercentageDeltaMLPredictorValue():
    meta: IPredictionMeta
    offset: float
    offsetFactor: float
    ocDelta: float
    ocDeltaFactor: float
    ohDelta: float
    ohDeltaFactor: float
    olDelta: float
    olDeltaFactor: float
