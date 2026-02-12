from Entities import INextCandlePrediction
from .MAPredictorValue import MAPredictorValue
from .MAPredictorAdapter import MAPredictorAdapter
from .SimpleMAPredictorAlgo import SimpleMAPredictorAlgo
from ..APredictor import APredictor

class MAPredictor(APredictor[MAPredictorValue, INextCandlePrediction]):

    def __init__(self, candlesCount: int):
        super().__init__(SimpleMAPredictorAlgo(candlesCount), MAPredictorAdapter())
