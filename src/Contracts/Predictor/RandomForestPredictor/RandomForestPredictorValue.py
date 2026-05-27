from dataclasses import dataclass, field
from typing import List, Tuple, Any
from Entities import PredictionMeta


@dataclass
class RandomForestPredictorValue:
    meta: PredictionMeta
    outputs: List[float] = field(default_factory=list)
    limits: List[Tuple[float, float]] = field(default_factory=lambda: [(0.0, 1.0)] * 4)

    def __post_init__(self) -> None:
        try:
            tolist = getattr(self.outputs, 'tolist', None)
            if callable(tolist):
                raw = tolist()
            else:
                raw = list(self.outputs)  # type: ignore

            raw_any: Any = raw
            if hasattr(raw_any, '__iter__'):
                try:
                    iterable = list(raw_any)  # type: ignore
                    self.outputs = [float(x) for x in iterable]
                except Exception:
                    self.outputs = [0.0, 0.0, 0.0, 0.0]
            else:
                try:
                    self.outputs = [float(raw_any)]
                except Exception:
                    self.outputs = [0.0, 0.0, 0.0, 0.0]
        except Exception:
            try:
                self.outputs = [float(self.outputs)]  # type: ignore
            except Exception:
                self.outputs = [0.0, 0.0, 0.0, 0.0]

        if len(self.outputs) != 4:
            raise ValueError("RandomForestPredictorValue.outputs must be a sequence of 4 floats")

        if not isinstance(self.limits, list) or len(self.limits) != 4:
            try:
                self.limits = list(self.limits)  # type: ignore
            except Exception:
                self.limits = [(0.0, 1.0)] * 4
