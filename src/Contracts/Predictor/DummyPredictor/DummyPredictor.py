from Entities import INextCandlePrediction
from .DummyPredictorValue import DummyPredictorValue
from .DummyPredictorAdapter import DummyPredictorAdapter
from .DummyPredictorAlgo import DummyPredictorAlgo
from ..APredictor import APredictor

class DummyPredictor(APredictor[DummyPredictorValue, INextCandlePrediction]):

    def __init__(self):
        super().__init__(DummyPredictorAlgo(), DummyPredictorAdapter(), 0)
