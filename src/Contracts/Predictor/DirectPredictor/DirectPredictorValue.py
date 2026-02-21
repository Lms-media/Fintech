from dataclasses import dataclass
from Interfaces import IPredictionMeta
from Interfaces import ICandle


@dataclass
class DirectPredictorValue():
    meta: IPredictionMeta
    nextCandle: ICandle
