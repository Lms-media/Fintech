from Entities import INextCandlePrediction
from .DirectPredictorValue import DirectPredictorValue
from .DirectPredictorAdapter import DirectPredictorAdapter
from .DirectPredictorAlgo import DirectPredictorAlgo
from ..APredictor import APredictor

class DirectPredictor(APredictor[DirectPredictorValue, INextCandlePrediction]):

    def __init__(self):
        super().__init__(DirectPredictorAlgo(), DirectPredictorAdapter())
