from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class DummyPredictorValue():
    meta: IPredictionMeta
