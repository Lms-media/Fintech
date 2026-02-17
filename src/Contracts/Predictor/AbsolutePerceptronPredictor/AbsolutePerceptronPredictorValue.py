from dataclasses import dataclass
from Interfaces import IPredictionMeta

@dataclass
class AbsolutePerceptronPredictorValue():
    meta: IPredictionMeta
    outputs: list[float]
    limits: list[tuple[float, float]]
