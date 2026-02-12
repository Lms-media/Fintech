from Entities import INextCandlePrediction
from .MAPredictorValue import MAPredictorValue
from .MAPredictorAdapter import MAPredictorAdapter
from .SimpleMAPredictorAlgo import SimpleMAPredictorAlgo
from ..APredictor import APredictor

class MAPredictor(APredictor[MAPredictorValue, INextCandlePrediction]):

    def __init__(self):
        super().__init__(SimpleMAPredictorAlgo(200), MAPredictorAdapter())
