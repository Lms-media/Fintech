from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class MAPredictorValue():
    meta: IPredictionMeta
    intersectionPrice: float
