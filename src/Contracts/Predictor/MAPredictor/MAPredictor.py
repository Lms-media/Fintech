from Entities import INextCandlePrediction
from .MAPredictorValue import MAPredictorValue
from .MAPredictorAdapter import MAPredictorAdapter
from .ExpMAPredictorAlgo import ExpMAPredictorAlgo
from ..APredictor import APredictor

class MAPredictor(APredictor[MAPredictorValue, INextCandlePrediction]):

    def __init__(self, candlesCount: int):
        super().__init__(ExpMAPredictorAlgo(candlesCount), MAPredictorAdapter())
