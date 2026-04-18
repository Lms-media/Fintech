from Entities import INextCandlePrediction
from Interfaces import IPredictorAlgo
from .IndicatorPredictorValue import IndicatorPredictorValue
from .IndicatorPredictorAdapter import IndicatorPredictorAdapter
from ..APredictor import APredictor

class IndicatorPredictor(APredictor[IndicatorPredictorValue, INextCandlePrediction]):

    def __init__(self, algo: IPredictorAlgo[IndicatorPredictorValue], candlesCount: int):
        super().__init__(algo, IndicatorPredictorAdapter(), candlesCount)
