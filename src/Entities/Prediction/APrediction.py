from abc import ABC
import uuid
from Interfaces import IPrediction, IPredictionMeta

class APrediction(IPrediction, ABC):
    _id: str
    _meta: IPredictionMeta

    def __init__(self, meta: IPredictionMeta):
        self._id = uuid.uuid4()
        self._meta = meta

    def getId(self) -> str:
        return self._id

    def getMeta(self) -> IPredictionMeta:
        return self._meta

    def __str__(self) -> str:
        return f"🔮 ({self.getId()}) Meta: {str(self.getMeta())}"
